<?php namespace HSkrasek\Queue\Jobs;

use Amp\Beanstalk\BeanstalkClient;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use function Amp\Promise\wait;

class AsyncBeanstalkdJob extends Job implements JobContract
{
    /**
     * @var BeanstalkClient
     */
    private $client;

    /**
     * @var int
     */
    private $id;

    /**
     * @var string
     */
    private $data;

    public function __construct(Container $container, BeanstalkClient $client, $id, $data, $connectionName, $queue)
    {
        $this->container      = $container;
        $this->client         = $client;
        $this->id             = $id;
        $this->data           = $data;
        $this->connectionName = $connectionName;
        $this->queue          = $queue;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int $delay
     *
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        wait($this->client->release($this->getId(), $delay));
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        wait($this->client->delete($this->getId()));
    }

    /**
     * Get the raw body of the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->data;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return wait($this->client->getJobStats($this->getId()))->reserves;
    }

    /**
     * @return int
     */
    public function getId(): int
    {
        return $this->id;
    }
}
