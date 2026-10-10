#include "redline/subscriber.hpp"

namespace redline {
class LambdaSubscriber : public Subscriber {
public:
  explicit LambdaSubscriber(std::function<void(const Message&)> callback)
      : callback_(std::move(callback)) {}

  void consume(const Message& message) override {
    callback_(message);
  }

private:
  std::function<void(const Message&)> callback_;
};

std::shared_ptr<Subscriber>
create_subscriber(const std::function<void(const Message &)> &callback) {
  return std::make_shared<LambdaSubscriber>(callback);
}
} // namespace redline
