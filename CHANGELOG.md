# Changelog

## 0.2.0
  * Streams that the credentials cannot access (401/403/404) are now excluded from the catalog during discovery instead of raising an error. [#14](https://github.com/singer-io/tap-copper/pull/14)

# 0.1.2
  * Bump requests to 2.33.0 for security updates [#15](https://github.com/singer-io/tap-copper/pull/15)
  * Fail discovery on invalid Copper credentials [#16](https://github.com/singer-io/tap-copper/pull/16)

## 0.1.1
  * Add singer.utils exception handling to main() [#11](https://github.com/singer-io/tap-copper/pull/11)

## 0.1.0
  * Full tap re-write [#6](https://github.com/singer-io/tap-copper/pull/6), [#7](https://github.com/singer-io/tap-copper/pull/7), [38](https://github.com/singer-io/tap-copper/pull/8)

## 0.0.1
  * Initial Commit
