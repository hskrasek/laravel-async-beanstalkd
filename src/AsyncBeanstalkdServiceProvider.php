<?php namespace HSkrasek\Queue;

use HSkrasek\Queue\Connectors\AsyncBeanstalkdConnector;
use Illuminate\Support\ServiceProvider;

class AsyncBeanstalkdServiceProvider extends ServiceProvider
{
    public function boot()
    {
        $this->app['queue']->addConnector('async-beanstalkd', function () {
            return new AsyncBeanstalkdConnector;
        });
    }
}
