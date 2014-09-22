<?php namespace Sv\LaravelRabbitmq;

use Illuminate\Queue\Queue;
use Illuminate\Queue\QueueInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMqQueue extends Queue implements QueueInterface
{

	/**
	 * The RabbitMqQueue instance.
	 *
	 * @var AMQPChannel
	 */
	protected $channel;

	/**
	 * The queue configuration
	 * @var array
	 */
	protected $queueConfig;


	/**
	 * Create a new Beanstalkd queue instance.
	 *
	 * @param  AMQPChannel $channel
	 * @param  string $queue
	 * @return void
	 */
	public function __construct(AMQPChannel $channel, $queueConfig)
	{
		$this->channel = $channel;
		$this->queueConfig = $queueConfig;
	}


	/**
	 * Create a payload string from the given job and data.
	 *
	 * @param  string $job
	 * @param  mixed $data
	 * @param int $attempts
	 * @return string
	 */
	protected function createPayload($job, $data = '', $attempts = 0)
	{
		if ($job instanceof Closure) {
			return json_encode($this->createClosurePayload($job, $data));
		} else {
			return json_encode(array('job' => $job, 'data' => $data, 'attempts' => $attempts));
		}
	}

	/**
	 * Create a payload string for the given Closure job.
	 *
	 * @param  \Closure $job
	 * @param  mixed $data
	 * @param int $attempts
	 * @return string
	 */
	protected function createClosurePayload($job, $data, $attempts = 0)
	{
		$closure = serialize(new SerializableClosure($job));

		return array('job' => 'IlluminateQueueClosure', 'data' => compact('closure'), 'attempts' => $attempts);
	}


	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string $job
	 * @param  mixed $data
	 * @param int $attempts
	 * @param  string $queue
	 * @return mixed
	 */
	public function _push($job, $data = '', $attempts = 0, $queue = null)
	{
		if (empty($queue)) $queue = $this->queueConfig['queue'];

		$this->channel->queue_declare($queue, false, $this->queueConfig['durable'], false, false);


		$payload = $this->createPayload($job, $data, $attempts);
		return $this->pushRaw($payload, $queue);
	}


	/**
	 * Push a raw payload onto the queue.
	 *
	 * @param  string $payload
	 * @param  string $queue
	 * @param  array $options
	 * @return mixed
	 */
	public function pushRaw($payload, $queue = null, array $options = array())
	{
		$properties = (sizeof($options) > 0 ? $options : null);
		if ($this->queueConfig['durable']) {
			$properties = array('delivery_mode' => 2); // delivery_mode = 2: persistent messages
		}

		$message = new AMQPMessage($payload, $properties);

		return $this->channel->basic_publish($message, '', $queue, false);
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string $job
	 * @param  mixed $data
	 * @param  string $queue
	 * @return mixed
	 */
	public function push($job, $data = '', $queue = null)
	{
		return $this->_push($job, $data, 0, $queue);
	}


	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int $delay
	 * @param  string $job
	 * @param  mixed $data
	 * @param  string $queue
	 * @return mixed
	 */
	public function later($delay, $job, $data = '', $queue = null)
	{
		return $this->_later($delay, $job, $data, 0, $queue);
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int $delay
	 * @param  string $job
	 * @param  mixed $data
	 * @param int $attempts
	 * @param  string $queue
	 * @return mixed
	 */
	public function _later($delay, $job, $data = '', $attempts = 0, $queue = null)
	{
		if ($delay == 0) return $this->push($job, $data, $queue);

		if (empty($queue)) $queue = $this->queueConfig['queue'];


		$delayedQueueName = $queue . "-delayed-" . md5(microtime(true) . rand(0, 9999999));
		$delay = $this->getSeconds($delay) * 1000;
		//var_dump($delayedQueueName);exit;

		$this->channel->exchange_declare("immediate", 'direct', false, false, false);

		$this->channel->queue_declare($delayedQueueName, false, $this->queueConfig['durable'], false, false, false, array(
			"x-dead-letter-exchange"    => array("S", ""),
			"x-dead-letter-routing-key" => array("S", $queue),
		));

		$payload = $this->createPayload($job, $data, $attempts);

		$properties = array(
			"expiration" => $delay,
		);

		if ($this->queueConfig['durable']) {
			$properties['delivery_mode'] = 2; // delivery_mode = 2: persistent messages
		}

		$message = new AMQPMessage($payload, $properties);

		return $this->channel->basic_publish($message, '', $delayedQueueName);
	}


	/**
	 * Pop the next job off of the queue.
	 *
	 * @param  string $queue
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = null)
	{
		if (empty($queue)) $queue = $this->queueConfig['queue'];

		$message = $this->channel->basic_get($queue);

		if ($message instanceof AMQPMessage) {
			$job = new RabbitMqJob($this->container, $this->channel, $this, $message);

			return $job;
		}
	}


}