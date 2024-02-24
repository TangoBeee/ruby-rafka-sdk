require 'bundler' ; Bundler.require
require './kafka_middleware'

use KafkaMiddleware, "requests", "tangobee123", 2


get '/' do
  "Hello World!"
end

not_found do
  status 404
  "Not Found!"
end
