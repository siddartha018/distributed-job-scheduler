#include <cctype>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

// Minimal, schema-specific parser:
// Expects JSON like: {"jobs":[{"status":"succeeded","execution_time_ms":123}, ...]}
// This avoids pulling in a large JSON dependency for a small CLI.

struct JobSummary {
  std::string status;
  long long execution_time_ms = -1;
};

static void skip_ws(const std::string& s, size_t& i) {
  while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) i++;
}

static bool consume(const std::string& s, size_t& i, char ch) {
  skip_ws(s, i);
  if (i < s.size() && s[i] == ch) {
    i++;
    return true;
  }
  return false;
}

static bool consume_literal(const std::string& s, size_t& i, const std::string& lit) {
  skip_ws(s, i);
  if (s.compare(i, lit.size(), lit) == 0) {
    i += lit.size();
    return true;
  }
  return false;
}

static bool parse_string(const std::string& s, size_t& i, std::string& out) {
  skip_ws(s, i);
  if (i >= s.size() || s[i] != '"') return false;
  i++;
  std::string result;
  while (i < s.size()) {
    char c = s[i++];
    if (c == '"') {
      out = result;
      return true;
    }
    if (c == '\\' && i < s.size()) {
      char e = s[i++];
      switch (e) {
        case '"': result.push_back('"'); break;
        case '\\': result.push_back('\\'); break;
        case '/': result.push_back('/'); break;
        case 'b': result.push_back('\b'); break;
        case 'f': result.push_back('\f'); break;
        case 'n': result.push_back('\n'); break;
        case 'r': result.push_back('\r'); break;
        case 't': result.push_back('\t'); break;
        default: return false;
      }
    } else {
      result.push_back(c);
    }
  }
  return false;
}

static bool parse_int(const std::string& s, size_t& i, long long& out) {
  skip_ws(s, i);
  size_t start = i;
  if (i < s.size() && (s[i] == '-' || s[i] == '+')) i++;
  bool any = false;
  while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) {
    any = true;
    i++;
  }
  if (!any) return false;
  try {
    out = std::stoll(s.substr(start, i - start));
    return true;
  } catch (...) {
    return false;
  }
}

static bool parse_jobs_array(const std::string& s, std::vector<JobSummary>& jobs) {
  size_t i = 0;
  // Find "jobs"
  while (i < s.size()) {
    std::string key;
    if (!parse_string(s, i, key)) { i++; continue; }
    if (key != "jobs") continue;
    if (!consume(s, i, ':')) return false;
    if (!consume(s, i, '[')) return false;
    skip_ws(s, i);
    if (consume(s, i, ']')) return true; // empty
    while (i < s.size()) {
      if (!consume(s, i, '{')) return false;
      JobSummary js;
      bool got_status = false;
      bool got_time = false;
      while (i < s.size()) {
        std::string k;
        if (!parse_string(s, i, k)) return false;
        if (!consume(s, i, ':')) return false;
        if (k == "status") {
          std::string v;
          if (!parse_string(s, i, v)) return false;
          js.status = v;
          got_status = true;
        } else if (k == "execution_time_ms") {
          long long v = -1;
          if (!parse_int(s, i, v)) return false;
          js.execution_time_ms = v;
          got_time = true;
        } else {
          // Skip a simple value (string/number/true/false/null/object/array) in a minimal way.
          skip_ws(s, i);
          if (i >= s.size()) return false;
          if (s[i] == '"') {
            std::string tmp;
            if (!parse_string(s, i, tmp)) return false;
          } else if (std::isdigit(static_cast<unsigned char>(s[i])) || s[i] == '-' || s[i] == '+') {
            long long tmp;
            if (!parse_int(s, i, tmp)) return false;
          } else if (consume_literal(s, i, "true") || consume_literal(s, i, "false") || consume_literal(s, i, "null")) {
            // ok
          } else {
            // For objects/arrays, skip by counting braces/brackets.
            char open = s[i];
            char close = (open == '{') ? '}' : (open == '[' ? ']' : '\0');
            if (close == '\0') return false;
            int depth = 0;
            do {
              if (i >= s.size()) return false;
              if (s[i] == open) depth++;
              else if (s[i] == close) depth--;
              i++;
            } while (depth > 0);
          }
        }
        skip_ws(s, i);
        if (consume(s, i, '}')) break;
        if (!consume(s, i, ',')) return false;
      }
      if (got_status && got_time) jobs.push_back(js);
      skip_ws(s, i);
      if (consume(s, i, ']')) return true;
      if (!consume(s, i, ',')) return false;
    }
    return false;
  }
  return false;
}

int main() {
  std::ostringstream buf;
  buf << std::cin.rdbuf();
  const std::string input = buf.str();

  std::vector<JobSummary> jobs;
  if (!parse_jobs_array(input, jobs)) {
    std::cerr << "failed_to_parse_jobs_json\n";
    return 2;
  }

  long long total = static_cast<long long>(jobs.size());
  long long succeeded = 0;
  long long failed = 0;
  long long sum_ms = 0;
  long long count_ms = 0;

  for (const auto& j : jobs) {
    if (j.status == "succeeded") succeeded++;
    if (j.status == "failed") failed++;
    if (j.execution_time_ms >= 0) {
      sum_ms += j.execution_time_ms;
      count_ms++;
    }
  }

  double success_rate = (total > 0) ? (static_cast<double>(succeeded) / static_cast<double>(total)) : 0.0;
  double failure_rate = (total > 0) ? (static_cast<double>(failed) / static_cast<double>(total)) : 0.0;
  double avg_ms = (count_ms > 0) ? (static_cast<double>(sum_ms) / static_cast<double>(count_ms)) : 0.0;

  std::cout << "{";
  std::cout << "\"total_jobs\":" << total << ",";
  std::cout << "\"success_rate\":" << success_rate << ",";
  std::cout << "\"failure_rate\":" << failure_rate << ",";
  std::cout << "\"avg_execution_time_ms\":" << avg_ms;
  std::cout << "}\n";
  return 0;
}

