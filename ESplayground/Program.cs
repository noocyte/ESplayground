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
                        SetAliasForIndex(localPrefix, d);
                    }


                }
            }

            // Provision
            var descriptors = GetIndexDescriptors().ToList();
            if (descriptors.Any(d => d.Prefix == prefix)) return; // this means we already have provisioned something...

            var articleDescriptor = IndexDescriptor.Articles(prefix, 0);
            _client.Indices.Create(articleDescriptor);
            SetAliasForIndex(prefix, articleDescriptor);

            var objectviewDescriptor = IndexDescriptor.Objectviews(prefix, 0);
            _client.Indices.Create(objectviewDescriptor);
            SetAliasForIndex(prefix, objectviewDescriptor);


            var nextTrack = NextEntityTrack();
            var maxGeneration = MaxEntityGeneration(nextTrack);
            var entitiesDescriptor = IndexDescriptor.Entities(nextTrack, maxGeneration);
            _client.Indices.Create(entitiesDescriptor);
            SetAliasForIndex(prefix, entitiesDescriptor);

            var alias = _client.Indices.GetAlias();
            descriptors = GetIndexDescriptors().ToList();

            // Provision Complete

            // Entity cleanup - max generations exceeded, move old to newer


            // create Articles and Objectview indexes
            // determine what generation and what track in entities to use
            // create alias for articles, objectview and entities


            alias = _client.Indices.GetAlias();
            // create first index for current prefix
            var currentGeneration = GetCurrentGenerationOrDefault(prefix, IndexDescriptor.ObjectViewTypeName);
            CreateObjectViewIndex(prefix, currentGeneration);
            SetAliasForObjectview(prefix, currentGeneration);

            // add some data to index

            var data = PlaygroundDataGenerator.Data(prefix).ToList();

            var indexResponse = BulkIndex(prefix, data);
            var searchResponse = FindAll(prefix);
            Console.WriteLine(searchResponse.Documents.Count);

            // pretend we want to reindex... 
            var nextGeneration = GetNextGeneration(prefix, IndexDescriptor.ObjectViewTypeName);
            CreateObjectViewIndex(prefix, nextGeneration);
            SetWriteToNextReadFromBoth(prefix, currentGeneration, nextGeneration);

            // write half data to new index
            alias = _client.Indices.GetAlias();

            //     var indexDescs = alias.Indices.Select(i =>  IndexDescriptor(i.Key.Name));

            indexResponse = BulkIndex(prefix, data.Take(5));
            searchResponse = FindAll(prefix);
            Console.WriteLine(searchResponse.Documents.Count);
            // should still be 10 documents
            // TODO: Change version for one, add one new - check if we get 11
            data.Last().Version = 1;
            data.Add(new Playground { Id = "10", Title = "New thing", Prefix = prefix });

            indexResponse = BulkIndex(prefix, data.Skip(5));

            // here reindex is complete, so we can set alias to just point to next and remove prev
            ResetAliasForObjectview(prefix, currentGeneration, nextGeneration);
            var indexTobeRemoved = IndexDescriptor.Objectviews(prefix, currentGeneration);

            searchResponse = FindAll(prefix);

            _client.Indices.Delete(indexTobeRemoved);

            searchResponse = FindAll(prefix);

            alias = _client.Indices.GetAlias();
        }

        private BulkResponse BulkIndex(string prefix, IEnumerable<Playground> foo)
        {
            return _client.Bulk(b => b
                    .Index(WriteAlias(prefix))
                    .IndexMany(foo, (descriptor, play) => descriptor
                        .VersionType(Elasticsearch.Net.VersionType.External)
                        .Version(play.Version)));
        }

        private ISearchResponse<Playground> FindAll(string prefix)
        {
            Thread.Sleep(200);
            return _client.Search<Playground>(d => d.MatchAll().Size(20).Index(ReadAlias(prefix)));
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
                .DefaultIfEmpty(new IndexDescriptor($"{indexTypeName}_{prefix}_0"))
                .Max(d => d.Generation);

            return maxCurrentGeneration;
        }

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

        private BulkAliasResponse SetAliasForIndex(string prefix, IndexDescriptor index)
        {
            return _client.Indices.BulkAlias(d => d
            .Add(a => a
                .Alias(WriteAlias(prefix))
                .Index(index)
                .Filter<Playground>(f => f.Term(p => p.Prefix, prefix)))
            .Add(a =>
                a.Alias(ReadAlias(prefix))
                .Indices(index)
                .Filter<Playground>(f => f.Term(p => p.Prefix, prefix)))
                        );
        }

        private BulkAliasResponse SetAliasForObjectview(string prefix, long gen1)
        {
            return _client.Indices.BulkAlias(d => d
            .Add(a => a
                       .Alias(WriteAlias(prefix))
                       .Index(IndexDescriptor.Objectviews(prefix, gen1))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(ReadAlias(prefix))
                        .Indices(IndexDescriptor.Objectviews(prefix, gen1))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }

        private static string ReadAlias(string prefix) => $"objectview_read_{prefix}";
        private static string WriteAlias(string prefix) => $"objectview_write_{prefix}";

        private BulkAliasResponse SetWriteToNextReadFromBoth(string prefix, long curr, long next)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(WriteAlias(prefix))
            .Indices(IndexDescriptor.Objectviews(prefix, curr))
            )
            .Add(a => a
                       .Alias(WriteAlias(prefix))
                       .Index(IndexDescriptor.Objectviews(prefix, next))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(ReadAlias(prefix))
                        .Indices(new string[] { IndexDescriptor.Objectviews(prefix, curr), IndexDescriptor.Objectviews(prefix, next) })
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }

        private BulkAliasResponse ResetAliasForObjectview(string prefix, long prev, long curr)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(WriteAlias(prefix))
            .Indices(IndexDescriptor.Objectviews(prefix, prev))
            )
            .Remove(r => r.Alias(ReadAlias(prefix))
            .Indices(IndexDescriptor.Objectviews(prefix, prev))
            )
            .Add(a => a
                       .Alias(WriteAlias(prefix))
                       .Index(IndexDescriptor.Objectviews(prefix, curr))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(ReadAlias(prefix))
                        .Indices(IndexDescriptor.Objectviews(prefix, curr))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                        )
                        );
        }


        private BulkAliasResponse SetAliasForObjectview(string prefix, long gen0, long gen1, long gen2)
        {
            return _client.Indices.BulkAlias(d => d
            .Remove(r => r.Alias(WriteAlias(prefix))
            .Indices(new string[] { IndexDescriptor.Objectviews(prefix, gen0), IndexDescriptor.Objectviews(prefix, gen1) })
            )
            .Remove(r => r.Alias(ReadAlias(prefix))
            .Indices(IndexDescriptor.Objectviews(prefix, gen0).ToString())
            )
            .Add(a => a
                       .Alias(WriteAlias(prefix))
                       .Index(IndexDescriptor.Objectviews(prefix, gen2))
                       .Filter<Playground>(f => f.Term(p => p.Prefix, prefix))
                       ).Add(a =>
                        a.Alias(ReadAlias(prefix))
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
        internal static readonly string ObjectViewTypeName = "objectview"; // objectview_prefix_generation
        internal static readonly string ArticleTypeName = "article";       // article_prefix_generation
        internal static readonly string EntityTypeName = "entity";         // entity_track_generation

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
