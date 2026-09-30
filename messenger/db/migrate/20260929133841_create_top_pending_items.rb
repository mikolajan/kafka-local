class CreateTopPendingItems < ActiveRecord::Migration[7.2]
  def change
    create_table :top_pending_items do |t|
      t.json :payload
    end
  end
end
