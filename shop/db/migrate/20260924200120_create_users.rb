class CreateUsers < ActiveRecord::Migration[7.2]
  def change
    create_table :users do |t|
      t.string :name
      t.string :phone
      t.string :email
      t.boolean :marketing_email, default: false, null: false
      t.boolean :marketing_sms, default: false, null: false

      t.timestamps
    end
  end
end
