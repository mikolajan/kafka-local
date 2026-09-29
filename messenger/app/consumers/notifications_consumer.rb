class NotificationsConsumer < Karafka::BaseConsumer
  def consume
    messages.each do |message|
      Rails.logger.info "Получено сообщение из топика notification: #{message.payload}"
      Notification.create!(payload: message.payload)
    end
  end
end
