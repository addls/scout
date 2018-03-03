<?php

namespace Laravel\Scout\Engines;

use Illuminate\Support\Facades\Schema;
use Laravel\Scout\Builder;
use Elasticsearch\Client as Elastic;
use Illuminate\Database\Eloquent\Collection;

class ElasticsearchEngine extends Engine
{
    /**
     * Index where the models will be saved.
     *
     * @var string
     */
    protected $index;

    /**
     * Elastic where the instance of Elastic|\Elasticsearch\Client is stored.
     *
     * @var object
     */
    protected $elastic;

    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elastic
     * @return void
     */
    public function __construct(Elastic $elastic, $index)
    {
        $this->elastic = $elastic;
        $this->index = $index;
    }

    /**
     * Update the given model in the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function update($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'update' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
            $params['body'][] = [
                'doc' => $model->toSearchableArray(),
                'doc_as_upsert' => true
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'size' => $builder->limit,
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        $result = $this->performSearch($builder, [
            'numericFilters' => $this->filters($builder),
            'from' => (($page * $perPage) - $perPage),
            'size' => $perPage,
        ]);

        $result['nbPages'] = $result['hits']['total']/$perPage;

        return $result;
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $filters = $must = $mustNot = $ranges = $should = [];

        $must[] = ['query_string' => [ 'query' => "*{$builder->query}*"]];

        if (array_key_exists('filters', $options) && $options['filters']) {
            foreach ($options['filters'] as $column => $value) {
                if(is_numeric($value)) {
                    $filters[] = [
                        'term' => [
                            $column => $value,
                        ]
                    ];
                } elseif(is_string($value)) {
                    $must[] = [
                        'term' => [
                            $column => $value
                        ]
                    ];
                }
            }
        }

        if (collect($builder->whereWithOperators)->count() > 0) {
            foreach ($builder->whereWithOperators as $key => $item) {
                $column = $item['column'];
                $value = $this->filterValue($builder, $column, $item['value']);
                $operator = str_replace('<>', '!=', strtolower($item['operator']));
                switch ($operator) {
                    case "=":
                        if(is_numeric($value)) {
                            $filters[] = [
                                'term' => [
                                    $column => $value,
                                ]
                            ];
                        } elseif(is_string($value)) {
                            $must[] = [
                                'term' => [
                                    $column => $value
                                ]
                            ];
                        }
                        break;
                    case "!=":
                        $mustNot[] = [
                            'term' => [
                                $column => $value,
                            ]
                        ];
                        break;
                    case ">":
                        //gt
                        $ranges[$column]['gt'] = $value;
                        break;
                    case ">=":
                        //gte
                        $ranges[$column]['gte'] = $value;
                        break;
                    case "<":
                        //lt
                        $ranges[$column]['lt'] = $value;
                        break;
                    case "<=":
                        //lte
                        $ranges[$column]['lte'] = $value;
                        break;
                    case "like":
                        //type phrase
                        $must[] = [
                            'match' => [
                                $column => [
                                    'query' => $value,
                                    'operator' => 'and'
                                ]
                            ]
                        ];
                        break;
                }
            }
        }

        collect($ranges)->count() > 0 && $must[]['range'] = $ranges;

        if (collect($builder->orWheres)->count() > 0) {
            foreach ($builder->orWheres as $key => $item) {
                $column = $item['column'];
                $value = $this->filterValue($builder, $column, $item['value']);
                $operator = str_replace('<>', '!=', strtolower($item['operator']));
                switch ($operator) {
                    case "=":
                        $should[] = ['match' => [$column => $value]];
                        break;
                    case ">":
                        //gt
                        $should[]['range'][$column]['gt'] = $value;
                        break;
                    case ">=":
                        //gte
                        $should[]['range'][$column]['gte'] = $value;
                        break;
                    case "<":
                        //lt
                        $should[]['range'][$column]['lt'] = $value;
                        break;
                    case "<=":
                        //lte
                        $should[]['range'][$column]['lte'] = $value;
                        break;
                    case "like":
                        $should[] = ['match' => [$column => $value]];
                        break;
                }
            }
        }

        if (collect($builder->whereIn)->count() > 0) {
            foreach ($builder->whereIn as $key => $item) {
                $values = $item['values'];
                if (! is_array($values)) {
                    $values = explode(',', $values);
                }
                foreach ($values as $value) {
                    $should[] = ['term' => [$item['column'] => $this->filterValue($builder, $item['column'], $value)]];
                }
            }
        }

        $params = [
            'index' => $this->index,
            'type' => $builder->index ?: $builder->model->searchableAs(),
            'body' => [
                'query' => [
                    'bool' => [
                        'filter' => $filters,
                        'must' => $must,
                        'must_not' => $mustNot,
                        'should' => $should
                    ]
                ]
            ]
        ];

        if (collect($should)->count() > 0) {
            $query['body']['query']['bool']['minimum_should_match'] = 1;
        }

        if ($sort = $this->sort($builder)) {
            $params['body']['sort'] = $sort;
        }

        if (isset($options['from'])) {
            $params['body']['from'] = $options['from'];
        }

        if (isset($options['size'])) {
            $params['body']['size'] = $options['size'];
        }

        if (isset($options['numericFilters']) && count($options['numericFilters'])) {
            $params['body']['query']['bool']['must'] = array_merge($params['body']['query']['bool']['must'], $options['numericFilters']);
        }

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elastic,
                $builder->query,
                $params
            );
        }

        return $this->elastic->search($params);
    }

    /**
     * filter value
     *
     * @param Builder $builder
     * @param string $column
     * @param string $value
     * @return mixed
     */
    protected function filterValue(Builder $builder, $column, $value)
    {
        $type = Schema::getColumnType($builder->model->getTable(), $column);
        $value = str_replace('%', '', $value);
        if ($type == 'integer') {
            $value = intval($value);
        }
        return $value;
    }

    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return collect($builder->wheres)->map(function ($value, $key) {
            if (is_array($value)) {
                return ['terms' => [$key => $value]];
            }

            return ['match_phrase' => [$key => $value]];
        })->values()->all();
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
    public function map($results, $model)
    {
        if ($results['hits']['total'] === 0) {
            return Collection::make();
        }

        $keys = collect($results['hits']['hits'])
            ->pluck('_id')->values()->all();

        $models = $model->whereIn(
            $model->getKeyName(), $keys
        )->get()->keyBy($model->getKeyName());

        return collect($results['hits']['hits'])->map(function ($hit) use ($model, $models) {
            return isset($models[$hit['_id']]) ? $models[$hit['_id']] : null;
        })->filter()->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return $results['hits']['total'];
    }

    /**
     * Generates the sort if theres any.
     *
     * @param  Builder $builder
     * @return array|null
     */
    protected function sort($builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }

        return collect($builder->orders)->map(function($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }
}