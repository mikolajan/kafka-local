Rails.application.routes.draw do
  resources :notifications, only: :index
  root "notifications#index"
end
