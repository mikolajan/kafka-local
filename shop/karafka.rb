class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { 'bootstrap.servers': 'kafka-broker:9092' }
    config.client_id = 'shop_producer_app'
  end

  routes.draw do
  end
end
