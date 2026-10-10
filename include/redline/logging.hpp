#pragma once

#include <functional>
#include <string_view>

namespace redline {
enum class LogLevel { INFO, WARNING, ERROR };

// Receives redline's diagnostics: segments being created or opened (INFO), subscribers being evicted or reclaimed
// (WARNING), and callbacks throwing or registration failing (ERROR). May be called from any thread, including poll
// threads, so it must be thread-safe and quick.
using LogHandler = std::function<void(LogLevel level, std::string_view message)>;

// Replaces the process-wide log handler. The default is default_log_handler; pass an empty handler to discard
// everything. Thread-safe.
void set_log_handler(LogHandler handler);

// Writes WARNING and ERROR to stderr and ignores INFO. Pass it to set_log_handler to restore the default.
void default_log_handler(LogLevel level, std::string_view message);
}
