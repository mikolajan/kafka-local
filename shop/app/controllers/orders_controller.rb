class OrdersController < ApplicationController
  def index
    @orders = Order.all.includes(:user)
  end

  def new
    @order = Order.new
  end

  def create
    @order = Order.new(order_params)

    respond_to do |format|
      if @order.save
        Karafka.producer.produce_async(
          topic: 'shop.orders', key: @order.kafka_number, payload: {
            number: @order.kafka_number, user_id: @order.user.kafka_user_id, status: @order.status,
            items: @order.items, created_at: @order.created_at
        }.to_json)

        format.html { redirect_to orders_path, notice: "Order was successfully created." }
      else
        format.html { render :new, status: :unprocessable_content }
      end
    end
  end

  private

  def order_params
    params.require(:order).permit(:user_id, items: [:name, :count]).tap do |permitted|
      permitted[:items].reject! { |item| item[:count].to_i == 0 } if permitted[:items].present?
    end
  end
end
