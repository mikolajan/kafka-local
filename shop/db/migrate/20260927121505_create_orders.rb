class CreateOrders < ActiveRecord::Migration[7.2]
  def change
    create_table :orders do |t|
      t.references :user, null: false, foreign_key: true
      t.string :number, null: false
      t.string :status, null: false, default: 'pending'
      t.jsonb :items, null: false, default: []

      t.timestamps
    end
  end
end
