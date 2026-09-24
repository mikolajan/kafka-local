class Order < ApplicationRecord
  belongs_to :user

  validates :status, presence: true, inclusion: { in: %w[pending] }
  validates :number, uniqueness: true, allow_blank: true
  validates :items, presence: true

  before_create :generate_number

  private

  def generate_number
    self.number = "SHOP-#{Date.today.strftime('%y')}-#{Time.current.to_i}"
  end
end
