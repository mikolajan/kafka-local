class User < ApplicationRecord
  validates :name, :email, :phone, presence: true

  def kafka_user_id
    "SHOP-USER-#{id}"
  end
end
