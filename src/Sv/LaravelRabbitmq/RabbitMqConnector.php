<?php namespace Sv\LaravelRabbitmq;

use Illuminate\Queue\Connectors\ConnectorInterface;
use PhpAmqpLib\Connection\AMQPConnection;

class RabbitMqConnector implements ConnectorInterface {

	/**
	 * Establish a queue connection.
	 *
	 * @param  array  $config
	 * @return \Illuminate\Queue\QueueInterface
	 */
	public function connect(array $config)
	{
		$connection = new AMQPConnection($config['host'], $config['port'], $config['user'], $config['pass']);

		return new RabbitMqQueue($connection->channel(), $config);
    }
}