Rails.application.routes.draw do
  resources :notifications, only: :index
  resources :top_pending_items, only: :index
  root "notifications#index"
end
