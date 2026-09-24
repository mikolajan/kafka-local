class User < ApplicationRecord
  validates :name, :email, :phone, presence: true
end
