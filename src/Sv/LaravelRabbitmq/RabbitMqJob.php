<?php namespace Sv\LaravelRabbitmq;

use Illuminate\Queue\Jobs\Job;
use Pheanstalk_Job;
use Illuminate\Container\Container;
use Pheanstalk_Pheanstalk as Pheanstalk;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Whoops\Exception\ErrorException;

class RabbitMqJob extends Job {

	/**
	 * The channel instance.
	 *
	 * @var AMQPChannel
	 */
	protected $channel;

    /**
     * The queue instance.
     *
     * @var RabbitMqQueue
     */
    protected $queue;


	/**
	 * The amqp message instance.
	 *
	 * @var AMQPMessage
	 */
	protected $message;

    /**
     * The data contained in the message
     * @var array
     */
    protected $data;

	/**
	 * Create a new job instance.
	 *
	 * @param  \Illuminate\Container\Container  $container
	 * @param  AMQPChannel  $channel
     * @param RabbitMqQueue $queue
	 * @param  AMQPMessage  $message
	 * @return void
	 */
	public function __construct(Container $container,
                                AMQPChannel $channel,
                                RabbitMqQueue $queue,
                                AMQPMessage $message)
	{
		$this->message = $message;
		$this->container = $container;
		$this->channel = $channel;
        $this->queue = $queue;

        $this->data = json_decode($this->message->body, true);
	}


	/**
	 * Fire the job.
	 *
	 * @return void
	 */
	public function fire()
	{
		//$this->resolveAndFire($this->data);
        try
        {
            list($class, $method) = $this->parseJob($this->data['job']);
        }
        catch (\ErrorException $e)
        {
            \Log::error("Queued job does not have a valid target: '" . serialize($this->data['job']) ."'");
            $this->delete();
            return;
        }

        try
        {
            $this->instance = $this->resolve($class);
        }
        catch (\ReflectionException $e)
        {
            \Log::error("Queued job does not have a valid target: '$class'");
            $this->delete();
            return;
        }

        $this->instance->{$method}($this, $this->data['data']);
	}

	/**
	 * Delete the job from the queue.
	 *
	 * @return void
	 */
	public function delete()
	{
        $this->channel->basic_ack($this->message->delivery_info['delivery_tag']);
	}

	/**
	 * Release the job back into the queue.
	 *
	 * @param  int   $delay
	 * @return void
	 */
	public function release($delay = 0)
	{
        if ($delay == 0) {
            $this->queue->_push($this->data['job'], $this->data['data'], $this->attempts()+1);
        }
        else{
            $this->queue->_later($delay, $this->data['job'], $this->data['data'], $this->attempts()+1);
        }
        $this->delete();
	}

	/**
	 * Get the number of times the job has been attempted.
	 *
	 * @return int
	 */
	public function attempts()
	{
		return (int) $this->data['attempts'];
	}

	/**
	 * Get the job identifier.
	 *
	 * @return string
	 */
	public function getJobId()
	{
		return $this->job->message_id;
	}

	/**
	 * Get the IoC container instance.
	 *
	 * @return \Illuminate\Container\Container
	 */
	public function getContainer()
	{
		return $this->container;
	}

	/**
	 * Get the underlying Pheanstalk instance.
	 *
	 * @return Pheanstalk
	 */
	public function getChannel()
	{
		return $this->channel;
	}

	/**
	 * Get the underlying Pheanstalk job.
	 *
	 * @return Pheanstalk_Job
	 */
	public function getAMQPMessage()
	{
		return $this->message;
	}

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return ''; // TODO
    }
}