class UsersController < ApplicationController
  def index
    @users = User.all
  end

  def new
    @user = User.new
  end

  def create
    @user = User.new(user_params)

    respond_to do |format|
      if @user.save
        Karafka.producer.produce_async(
          topic: 'shop.users', key: @user.kafka_user_id, payload: {
            user_id: @user.kafka_user_id, email: @user.email, phone: @user.phone,
            marketing_email: @user.marketing_email?, marketing_sms: @user.marketing_sms?
        }.to_json)

        format.html { redirect_to users_path, notice: "User was successfully created." }
      else
        format.html { render :new, status: :unprocessable_content }
      end
    end
  end

  private

  def user_params
    params.require(:user).permit(:name, :phone, :email, :marketing_email, :marketing_sms)
  end
end
