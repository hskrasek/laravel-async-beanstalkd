<?php namespace HSkrasek\Queue\Connectors;

use Amp\Beanstalk\BeanstalkClient;
use HSkrasek\Queue\AsyncBeanstalkdQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

class AsyncBeanstalkdConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     *
     * @param  array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        return new AsyncBeanstalkdQueue(
            $this->beanstalkClient($config),
            $config['queue'],
            $config['retry_after'] ?? 60
        );
    }

    protected function beanstalkClient(array $config)
    {
        return new BeanstalkClient(
            "tcp://{$config['host']}:{$config['port']}?tube={$config['queue']}"
        );
    }
}
