<?php

namespace Laravel\Scout;

use Illuminate\Support\Manager;
use AlgoliaSearch\Client as Algolia;
use Laravel\Scout\Engines\NullEngine;
use Laravel\Scout\Engines\AlgoliaEngine;
use AlgoliaSearch\Version as AlgoliaUserAgent;
use Laravel\Scout\Engines\ElasticsearchEngine;
use Elasticsearch\ClientBuilder as ElasticBuilder;

class EngineManager extends Manager
{
    /**
     * Get a driver instance.
     *
     * @param  string|null  $name
     * @return mixed
     */
    public function engine($name = null)
    {
        return $this->driver($name);
    }

    /**
     * Create an Algolia engine instance.
     *
     * @return \Laravel\Scout\Engines\AlgoliaEngine
     */
    public function createAlgoliaDriver()
    {
        AlgoliaUserAgent::addSuffixUserAgentSegment('Laravel Scout', '3.0.10');

        return new AlgoliaEngine(new Algolia(
            config('scout.algolia.id'), config('scout.algolia.secret')
        ));
    }

    /**
     * Create an Elasticsearch engine instance.
     *
     * @return \Laravel\Scout\Engines\ElasticsearchEngine
     */
    public function createElasticsearchDriver()
    {
        return new ElasticsearchEngine(ElasticBuilder::create()
            ->setHosts(config('scout.elasticsearch.hosts'))
            ->build(),
            config('scout.elasticsearch.index')
        );
    }

    /**
     * Create a Null engine instance.
     *
     * @return \Laravel\Scout\Engines\NullEngine
     */
    public function createNullDriver()
    {
        return new NullEngine;
    }

    /**
     * Get the default session driver name.
     *
     * @return string
     */
    public function getDefaultDriver()
    {
        if (is_null($this->app['config']['scout.driver'])) {
            return 'null';
        }

        return $this->app['config']['scout.driver'];
    }
}
