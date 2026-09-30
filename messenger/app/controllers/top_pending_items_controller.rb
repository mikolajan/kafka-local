class TopPendingItemsController < ApplicationController
  def index
    @top_pending_items = TopPendingItem.order(id: :desc).limit(5)
  end
end
