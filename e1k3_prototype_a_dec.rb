# e1k3_prototype_a_dec.rb

require 'json'
require 'yaml'
require 'net/http'
require 'uri'
require 'securerandom'

# Configuration
config = {
  # Data Sources
  data_sources: [
    {
      name: "Alpha",
      type: "REST",
      url: "https://alpha.com/api/data",
      auth: {
        username: "alpha_user",
        password: "alpha_pass"
      }
    },
    {
      name: "Bravo",
      type: "GraphQL",
      url: "https://bravo.com/graphql",
      auth: {
        token: "bravo_token"
      }
    }
  ],

  # Data Processors
  data_processors: [
    {
      name: "Filter",
      type: "ruby",
      code: "data.select { |d| d[:age] > 18 }"
    },
    {
      name: "Mapper",
      type: "ruby",
      code: "data.map { |d| { name: d[:name], email: d[:email] } }"
    }
  ],

  # Data Sinks
  data_sinks: [
    {
      name: "Elasticsearch",
      type: "elasticsearch",
      url: "https://elasticsearch.com/index",
      auth: {
        username: "elastic_user",
        password: "elastic_pass"
      }
    },
    {
      name: "Kafka",
      type: "kafka",
      url: "kafka://kafka.com:9092",
      topic: "my_topic"
    }
  ],

  # Pipeline Configuration
  pipeline: {
    data_source: "Alpha",
    data_processors: ["Filter", "Mapper"],
    data_sink: "Elasticsearch"
  }
}

# Functions
def generate_data_source(data_source)
  case data_source[:type]
  when "REST"
    uri = URI.parse(data_source[:url])
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    request = Net::HTTP::Get.new(uri.request_uri)
    request.basic_auth(data_source[:auth][:username], data_source[:auth][:password])
    response = http.request(request)
    JSON.parse(response.body)
  when "GraphQL"
    url = URI.parse(data_source[:url])
    http = Net::HTTP.new(url.host, url.port)
    http.use_ssl = true
    request = Net::HTTP::Post.new(url.request_uri)
    request["Content-Type"] = "application/json"
    request.body = { query: "query { data { id name email } }" }.to_json
    request["Authorization"] = "Bearer #{data_source[:auth][:token]}"
    response = http.request(request)
    JSON.parse(response.body)["data"]["data"]
  end
end

def process_data(data, data_processors)
  data_processors.each do |dp|
    case dp[:type]
    when "ruby"
      data = eval(dp[:code])
    end
  end
  data
end

def sink_data(data, data_sink)
  case data_sink[:type]
  when "elasticsearch"
    uri = URI.parse(data_sink[:url])
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    data.each do |d|
      request = Net::HTTP::Post.new(uri.request_uri)
      request.body = d.to_json
      request.basic_auth(data_sink[:auth][:username], data_sink[:auth][:password])
      http.request(request)
    end
  when "kafka"
    kafka = Kafka.new(data_sink[:url], data_sink[:topic])
    data.each do |d|
      kafka.produce(d.to_json)
    end
  end
end

# Pipeline
data = generate_data_source(config[:pipeline][:data_source])
data = process_data(data, config[:data_processors].select { |dp| config[:pipeline][:data_processors].include?(dp[:name]) })
sink_data(data, config[:pipeline][:data_sink])