Rails.application.routes.draw do
  resources :users, only: %i[new create index]
  resources :orders, only: %i[new create index ]
  root "orders#index"
end
