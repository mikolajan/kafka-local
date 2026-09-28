class Order < ApplicationRecord
  belongs_to :user

  validates :status, presence: true, inclusion: { in: %w[pending] }
  validates :items, presence: true

  def kafka_number
    "SHOP-ORDER-#{id}"
  end
end
