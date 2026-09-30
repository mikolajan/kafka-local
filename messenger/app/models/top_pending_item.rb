class TopPendingItem < ApplicationRecord
  store_accessor :payload, :window_end, :window_start, :items
end
