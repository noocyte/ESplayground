using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ESplayground
{
    class Program
    {
        const int generation = 1;
        IElasticClient _client;
        static void Main(string[] args)
        {
            new Program().RunIt();
        }

        void RunIt()
        {
            // new prefix every run - keep it easy :P
            var prefixGenerator = new Fare.Xeger("^[a-z][a-z0-9]{7}$");
            var prefix = prefixGenerator.Generate();
            var settings = new ConnectionSettings(new Uri("http://127.0.0.1:7200"));
            //var lowClient = new ElasticLowLevelClient(settings);

            _client = new ElasticClient(settings);

            RemoveAllIndices();

            // create some random entity things... 
            var random = new Random();
            for (int track = 0; track < 5; track++)
            {
                var maxGen = random.Next(3);
                for (int generation = 0; generation <= maxGen; generation++)
                {
                    var d = IndexDescriptor.Entities(track, generation);
                    _client.Indices.Create(d);

                    var numberOfOrgs = random.Next(7);
                    for (int i = 0; i < numberOfOrgs; i++)
                    {
                        var localPrefix = prefixGenerator.Generate();
                        var localAlias = new AliasNames(localPrefix);
                        SetAliasForIndex(localPrefix, d, localAlias);
                    }


                }
            }

            // Provision
            var descriptors = GetIndexDescriptors().ToList();
            if (descriptors.Any(d => d.Prefix == prefix)) return; // this means we already have provisioned something...

            var prefixAlias = new AliasNames(prefix);

            var articleDescriptor = IndexDescriptor.Articles(prefix, 0);
            _client.Indices.Create(articleDescriptor);
            SetAliasForIndex(prefix, articleDescriptor, prefixAlias);

            var objectviewDescriptor = IndexDescriptor.Objectviews(prefix, 0);
            _client.Indices.Create(objectviewDescriptor);
            SetAliasForIndex(prefix, objectviewDescriptor, prefixAlias);


            var nextTrack = NextEntityTrack();
            var maxGeneration = MaxEntityGeneration(nextTrack);
            var entitiesDescriptor = IndexDescriptor.Entities(nextTrack, maxGeneration);
            _client.Indices.Create(entitiesDescriptor);
            SetAliasForIndex(prefix, entitiesDescriptor, prefixAlias);

            var alias = _client.Indices.GetAlias();
            descriptors = GetIndexDescriptors().ToList();

            // Provision Complete

            // Entity cleanup - max generations exceeded, move old to newer


            // create Articles and Objectview indexes
            // determine what generation and what track in entities to use
            // create alias for articles, objectview and entities


            alias = _client.Indices.GetAlias();
            var aliasNames = new AliasNames(prefix);
            // create first index for current prefix
            var currentGeneration = GetCurrentGenerationOrDefault(prefix, IndexDescriptor.ObjectViewTypeName);
            CreateObjectViewIndex(prefix, currentGeneration);
            SetAliasForObjectview(prefix, currentGeneration, prefixAlias);

            // add some data to index

            var data = PlaygroundDataGenerator.Data(prefix).ToList();

            var indexResponse = BulkIndexObjectview(prefix, data, aliasNames);
            var searchResponse = FindAll(prefixAlias);
            Console.WriteLine(searchResponse.Documents.Count);

            // pretend we want to reindex... 
            var nextGeneration = GetNextGeneration(prefix, IndexDescriptor.ObjectViewTypeName);
            CreateObjectViewIndex(prefix, nextGeneration);
            SetWriteToNextReadFromBoth(prefix, currentGeneration, nextGeneration, prefixAlias);

            // write half data to new index
            alias = _client.Indices.GetAlias();

            //     var indexDescs = alias.Indices.Select(i =>  IndexDescriptor(i.Key.Name));

            indexResponse = BulkIndexObjectview(prefix, data.Take(5), aliasNames);
            searchResponse = FindAll(prefixAlias);
            Console.WriteLine(searchResponse.Documents.Count);
            // should still be 10 documents
            // TODO: Change version for one, add one new - check if we get 11
            data.Last().Version = 1;
            data.Add(new Playground { Id = "10", Title = "New thing", Prefix = prefix });

            indexResponse = BulkIndexObjectview(prefix, data.Skip(5), aliasNames);

            // here reindex is complete, so we can set alias to just point to next and remove prev
            ResetAliasForObjectview(prefix, currentGeneration, nextGeneration, prefixAlias);
            var indexTobeRemoved = IndexDescriptor.Objectviews(prefix, currentGeneration);

            searchResponse = FindAll(prefixAlias);

            _client.Indices.Delete(indexTobeRemoved);

            searchResponse = FindAll(prefixAlias);

            alias = _client.Indices.GetAlias();
        }

        private BulkResponse BulkIndexObjectview(string prefix, IEnumerable<Playground> foo, AliasNames alias)
        {
            return _client.Bulk(b => b
                    .Index(alias.ObjectviewWrite)
                    .IndexMany(foo, (descriptor, play) => descriptor
                        .VersionType(Elasticsearch.Net.VersionType.External)
                        .Version(play.Version)));
        }

        private ISearchResponse<Playground> FindAll(AliasNames alias)
        {
            Thread.Sleep(200);
            return _client.Search<Playground>(d => d.MatchAll().Size(20).Index(alias.ObjectviewRead));
        }

        private long GetNextGeneration(string prefix, string indexTypeName)
        {
            var maxCurrentGeneration = GetIndexDescriptors()
                .Where(d => d.Prefix == prefix)
                .Where(d => d.IndexTypeName == indexTypeName)
                .Max(d => d.Generation);

            var nx = maxCurrentGeneration + 1;
            return nx;
        }

        private long GetCurrentGenerationOrDefault(string prefix, string indexTypeName)
        {
            var descriptors = GetIndexDescriptors();
            var maxCurrentGeneration = descriptors
                .Where(d => d.Prefix == prefix)
                .DefaultIfEmpty(ToIndexDescriptor(indexTypeName, prefix))
                .Max(d => d.Generation);

            return maxCurrentGeneration;
        }

        public static IndexDescriptor ToIndexDescriptor(string indexTypeName, string prefix, long generation = 0, long track = 0) => indexTypeName switch
        {
            IndexDescriptor.ArticleTypeName => IndexDescriptor.Articles(prefix, generation),
            IndexDescriptor.ObjectViewTypeName => IndexDescriptor.Objectviews(prefix, generation),
            IndexDescriptor.EntityTypeName => IndexDescriptor.Entities(track, generation),
            _ => throw new ArgumentOutOfRangeException(nameof(indexTypeName), $"Not expected indexTypeName value: {indexTypeName}"),
        };

        private IEnumerable<IndexDescriptor> GetIndexDescriptors()
        {
            return _client.Indices.GetAlias().Indices
                .Select(kvp => new IndexDescriptor(kvp.Key.ToString()))
                .OrderBy(d => d.IndexTypeName)
                .ThenBy(d => d.Track)
                .ThenBy(d => d.Prefix)
                .ThenBy(d => d.Generation);
        }

        private long NextEntityTrack()
        {
            var nextEntityTrack = _client.Indices
                .GetAlias().Indices
                .Select(kvp => new { Descriptor = new IndexDescriptor(kvp.Key.ToString()), Count = kvp.Value.Aliases.Count })
                .Where(a => a.Descriptor.IsEntityIndex)
                .GroupBy(a => a.Descriptor.Track)
                .Min(a => a.Sum(b => b.Count));

            return nextEntityTrack;
        }

        private long MaxEntityGeneration(long track)
        {
            var nextEntityGenerationForTrack = _client.Indices.GetAlias().Indices
                .Select(kvp => new IndexDescriptor(kvp.Key.ToString()))
                .Where(a => a.IsEntityIndex)
                .Where(a => a.Track == track)
                .Max(a => a.Generation);

            return nextEntityGenerationForTrack;
        }

        private void RemoveAllIndices()
        {
            foreach (var a in _client.Indices.GetAlias().Indices)
            {
                _client.Indices.Delete(a.Key);
            }
        }

        private BulkAliasResponse SetAliasForIndex(string prefix, IndexDescriptor index, AliasNames alias)
        {
            return _client.Indices.BulkAlias(d => d
            .Add(a => a
                .Alias(alias.ObjectviewWrite)
                .Index(index)
                .Filter<Playground>(f => f.Term(p => p.Prefix, prefix)))
            .Add(a =>
                a.Alias(alias.ObjectviewRead)
                .Indices(index)
                .Filter<Playground>(f => f.Term(p => p.Prefix, prefix)))
            );
        }

        private BulkAliasResponse SetAliasForObjectview(string prefix, long gen1, AliasNames alias)
        {
            return _client.Indices.BulkAlias(d => d
            .Add(a => a
                       .Alias(alias.ObjectviewWrite)
                       .Index(IndexDescriptor.Objectviews(prefix, gen1))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(alias.ObjectviewRead)
                        .Indices(IndexDescriptor.Objectviews(prefix, gen1))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }

        private BulkAliasResponse SetWriteToNextReadFromBoth(string prefix, long curr, long next, AliasNames alias)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(alias.ObjectviewWrite)
            .Indices(IndexDescriptor.Objectviews(prefix, curr))
            )
            .Add(a => a
                       .Alias(alias.ObjectviewWrite)
                       .Index(IndexDescriptor.Objectviews(prefix, next))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(alias.ObjectviewRead)
                        .Indices(new string[] { IndexDescriptor.Objectviews(prefix, curr), IndexDescriptor.Objectviews(prefix, next) })
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }

        private BulkAliasResponse ResetAliasForObjectview(string prefix, long prev, long curr, AliasNames alias)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(alias.ObjectviewWrite)
            .Indices(IndexDescriptor.Objectviews(prefix, prev))
            )
            .Remove(r => r.Alias(alias.ObjectviewRead)
            .Indices(IndexDescriptor.Objectviews(prefix, prev))
            )
            .Add(a => a
                       .Alias(alias.ObjectviewWrite)
                       .Index(IndexDescriptor.Objectviews(prefix, curr))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(alias.ObjectviewRead)
                        .Indices(IndexDescriptor.Objectviews(prefix, curr))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }


        private BulkAliasResponse SetAliasForObjectview(string prefix, long gen0, long gen1, long gen2, AliasNames alias)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(alias.ObjectviewWrite)
            .Indices(new string[] { IndexDescriptor.Objectviews(prefix, gen0), IndexDescriptor.Objectviews(prefix, gen1) })
            )
            .Remove(r => r.Alias(alias.ObjectviewRead)
            .Indices(IndexDescriptor.Objectviews(prefix, gen0).ToString())
            )
            .Add(a => a
                       .Alias(alias.ObjectviewWrite)
                       .Index(IndexDescriptor.Objectviews(prefix, gen2))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(alias.ObjectviewRead)
                        .Indices(new string[] { IndexDescriptor.Objectviews(prefix, gen1), IndexDescriptor.Objectviews(prefix, gen2) })
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }

        void CreateObjectViewIndex(string prefix, long generation)
        {
            var indexName = IndexDescriptor.Objectviews(prefix, generation);
            var indexResponse = _client.Indices.Create(indexName);
        }
    }

    public class IndexDescriptor
    {
        internal const string ObjectViewTypeName = "objectview"; // objectview_prefix_generation
        internal const string ArticleTypeName = "article";       // article_prefix_generation
        internal const string EntityTypeName = "entity";         // entity_track_generation

        public bool IsEntityIndex => IndexTypeName.Equals(EntityTypeName);

        public static IndexDescriptor Objectviews(string prefix, long generation)
            => new(ObjectViewTypeName, prefix, generation);

        public static IndexDescriptor Entities(long track, long generation)
            => new(EntityTypeName, string.Empty, generation, track);

        public static IndexDescriptor Articles(string prefix, long generation)
            => new(ArticleTypeName, prefix, generation);

        private IndexDescriptor(string indexTypeName, string prefix, long generation, long track = 0)
        {
            IndexTypeName = indexTypeName;
            Prefix = prefix;
            Generation = generation;
            Track = track;
        }

        public IndexDescriptor(string indicesString)
        {
            var split = indicesString.Split('_');
            IndexTypeName = split[0];
            Generation = long.Parse(split[2]);
            if (!IsEntityIndex)
            {
                Prefix = split[1];
                Track = 0;
            }
            else
            {
                Prefix = string.Empty;
                Track = long.Parse(split[1]);
            }

        }
        public string IndexTypeName { get; set; }
        public string Prefix { get; set; }
        public long Generation { get; set; }
        public long Track { get; set; }

        public static implicit operator string(IndexDescriptor desc) => desc.ToString();

        public static implicit operator IndexName(IndexDescriptor desc) => desc.ToString();

        public static implicit operator Indices(IndexDescriptor desc) => desc.ToString();

        public static implicit operator IndexDescriptor(IndexName desc) => new(desc.ToString());

        public static implicit operator IndexDescriptor(string indicesString) => new(indicesString);

        public override string ToString()
        {
            return IsEntityIndex
              ? $"{IndexTypeName}_{Track}_{Generation}"
              : $"{IndexTypeName}_{Prefix}_{Generation}";
        }
    }

    public record AliasNames
    {
        public AliasNames(string prefix)
        {
            var write = "write";
            var read = "read";

            EntityRead = $"{IndexDescriptor.EntityTypeName}_{read}_{prefix}";
            EntityWrite = $"{IndexDescriptor.EntityTypeName}_{write}_{prefix}";

            ArticleRead = $"{IndexDescriptor.ArticleTypeName}_{read}_{prefix}";
            ArticleWrite = $"{IndexDescriptor.ArticleTypeName}_{write}_{prefix}";

            ObjectviewRead = $"{IndexDescriptor.ObjectViewTypeName}_{read}_{prefix}";
            ObjectviewWrite = $"{IndexDescriptor.ObjectViewTypeName}_{write}_{prefix}";
        }

        public string EntityRead { get; }
        public string EntityWrite { get; }

        public string ObjectviewRead { get; }
        public string ObjectviewWrite { get; }

        public string ArticleRead { get; }
        public string ArticleWrite { get; }
    }


    public class Playground
    {
        public string Id { get; set; }
        public string Prefix { get; set; }
        public string Title { get; set; }
        public long Version { get; set; }
    }

    public static class PlaygroundDataGenerator
    {
        public static IEnumerable<Playground> Data(string prefix)
        {
            var prefixGenerator = new Fare.Xeger("^[a-z][a-z0-9]{7}$");
            for (int i = 0; i < 10; i++)
            {
                yield return new Playground { Id = $"{i}", Prefix = prefix, Title = $"{i} - {prefixGenerator.Generate()}" };
            }
        }
    }
}
