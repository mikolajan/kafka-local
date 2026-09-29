class NotificationsController < ApplicationController
  def index
    @notifications = Notification.order(id: :desc).limit(15)
  end
end
