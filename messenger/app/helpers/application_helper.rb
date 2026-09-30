module ApplicationHelper
  def long_time_from_payload(t)
    if t.present?
      l(Time.zone.parse(t), format: :long) rescue nil
    end
  end
end
