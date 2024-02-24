require 'kafka'
require 'json'

class KafkaMiddleware
  def initialize(app, topic, userid, buffer_limit, kafka_brokers: ["localhost:9092"])
    @app = app
    @topic = topic
    @userid = userid
    @buffer_limit = buffer_limit
    @messages = []
    @kafka_brokers = kafka_brokers
    @kafka_producer = create_kafka_producer
  end

  def call(env)
    status, headers, response = @app.call(env)
    request = Rack::Request.new(env)

    logs = gen_logs(request, response, env)
    @messages << logs

    if @messages.size >= @buffer_limit
      send_to_kafka
      @messages.clear
    end

    [status, headers, response]
  end

  private

  def gen_logs(request, response, env)
    {
      userid: @userid,
      path: request.path,
      requestHeaders: request.env.select { |k, _| k.start_with?('HTTP_') }.to_json,
      method: request.request_method,
      requestPayload: request.body.read,
      ip: env['HTTP_X_FORWARDED_FOR'] || env['REMOTE_ADDR'],
      time: Time.now.to_i.to_s,
      type: "HTTP/#{env['HTTP_VERSION']}"
    }
  end

  def send_to_kafka
    @kafka_producer.produce(@messages.join("\n"), topic: @topic)
    @kafka_producer.deliver_messages
  end

  def create_kafka_producer
    kafka = Kafka.new(seed_brokers: @kafka_brokers)
    kafka.producer
  end
end
