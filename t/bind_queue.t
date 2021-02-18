#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 6;
use Log::Log4perl;

# Test binding a queue to an exchange via config
# and making sure logs get sent to RabbitMQ

my $conf = <<CONF;
    log4perl.category.cat1 = INFO, RabbitMQ

    log4perl.appender.RabbitMQ=Log::Log4perl::Appender::RabbitMQ

    # turn on testing mode, so that we won't really try to
    # connect to a RabbitMQ, but will use Test::Net::RabbitMQ instead
    log4perl.appender.RabbitMQ.TESTING=1

    log4perl.appender.RabbitMQ.declare_exchange=1
    log4perl.appender.RabbitMQ.exchange=myexchange
    log4perl.appender.RabbitMQ.routing_key=myqueue
    log4perl.appender.RabbitMQ.queue=myqueue
    log4perl.appender.RabbitMQ.exclusive_queue=1

    log4perl.appender.RabbitMQ.layout=PatternLayout
    log4perl.appender.RabbitMQ.layout.ConversionPattern=%p>%m%n
CONF

Log::Log4perl->init(\$conf);

# Get the appender Object
my $appender = Log::Log4perl->appenders->{RabbitMQ};#DEBUG#

isa_ok($appender, 'Log::Log4perl::Appender', 'RabbitMQ appender');

# Get the RabbitMQ object
my $mq = $appender->{appender}->_connect_cached();

# Make sure the exchange got declared
ok($mq->_get_exchange("myexchange"), "declare_exchange respected");
ok($mq->_get_queue("myqueue"), "queue created");
is_deeply([values %{$mq->bindings->{myexchange}}], ['myqueue'], "exchange bound to queue");

#Unfortunately, Test::Net::RabbitMQ does not save queue options, so we can't test those properly
is_deeply($appender->{appender}->{queue_options}, {exclusive => 1}, "queue options are passed on");

# Open a second channel to consume the messages.
$mq->channel_open(2);
$mq->consume(2, "myqueue");

# Do some logging, checking the queue after each
my $logger = Log::Log4perl->get_logger('cat1');

$logger->info("info message 1 ");
is_deeply(
    $mq->recv(),
    {
        body         => "INFO>info message 1 \n",
        routing_key  => 'myqueue',
        exchange     => 'myexchange',
        delivery_tag => 1,
        consumer_tag => '',
        props        => {},
        redelivered  => 0,
    },
    "info message sent to Rabbit with proper format");
