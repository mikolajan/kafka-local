class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { 'bootstrap.servers': 'kafka-broker:9092' }
    config.client_id = 'messenger_consumer_app'
  end

  routes.draw do
    consumer_group :messenger_notifications_group do
      topic 'shop.notifications' do
        consumer NotificationsConsumer
      end
    end
  end
end
