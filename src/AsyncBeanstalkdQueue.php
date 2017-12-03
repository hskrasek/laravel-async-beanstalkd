<?php namespace HSkrasek\Queue;

use Amp\Beanstalk\BeanstalkClient;
use HSkrasek\Queue\Jobs\AsyncBeanstalkdJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use function Amp\call;
use function Amp\Promise\wait;

class AsyncBeanstalkdQueue extends Queue implements QueueContract
{
    /**
     * @var BeanstalkClient
     */
    protected $client;

    /**
     * @var string
     */
    protected $default;

    /**
     * @var int
     */
    protected $timeToRun;

    public function __construct(BeanstalkClient $client, $default, $timeToRun)
    {
        $this->client    = $client;
        $this->default   = $default;
        $this->timeToRun = $timeToRun;
    }

    /**
     * Get the size of the queue.
     *
     * @param  string $queue
     *
     * @return int
     */
    public function size($queue = null)
    {
        return wait(call(function () use ($queue) {
            return $this->client->getTubeStats($this->getQueue($queue));
        }))->currentJobsUrgent;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string|object $job
     * @param  mixed $data
     * @param  string $queue
     *
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string $queue
     * @param  array $options
     *
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        call(function () use ($queue) {
            $this->client->use($this->getQueue($queue));
        });

        return wait(call(function () use ($payload) {
            return $this->client->put($payload, $this->timeToRun);
        }));
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTimeInterface|\DateInterval|int $delay
     * @param  string|object $job
     * @param  mixed $data
     * @param  string $queue
     *
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        call(function () use ($queue) {
            $this->client->use($this->getQueue($queue));
        });

        return wait(call(function () use ($delay, $job, $data) {
            return $this->client->put(
                $this->createPayload($job, $data),
                $this->timeToRun,
                $this->secondsUntil($delay)
            );
        }));
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string $queue
     *
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        call(function () use ($queue) {
            $this->client->watch($queue);
        });

        if ([$jobId, $jobBody] = wait(call(function () {
            return $this->client->reserve($this->timeToRun);
        }))) {
            return new AsyncBeanstalkdJob(
                $this->container,
                $this->client,
                $jobId,
                $jobBody,
                $this->connectionName,
                $queue
            );
        }
    }

    /**
     * Delete a message from the Beanstalk queue.
     *
     * @param  string $queue
     * @param  string $id
     *
     * @return void
     */
    public function deleteMessage($queue, $id)
    {
        call(function () use ($queue) {
            $this->client->use($this->getQueue($queue));
        });

        $this->client->delete($id);
    }

    /**
     * Get the queue or return the default.
     *
     * @param  string|null $queue
     *
     * @return string
     */
    public function getQueue($queue)
    {
        return $queue ?? $this->default;
    }
}
