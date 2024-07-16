# cachec

`cachec` is a caching utility package in Go, designed to provide a simple and efficient caching mechanism for Go applications. This package offers thread-safe in-memory caching with customizable expiration times.

## Features

- Thread-safe in-memory caching
- Configurable expiration times for cached items
- Automatic cleanup of expired items
- Simple and easy-to-use API

## Installation

To install `cachec` using `go mod` and `vendor`:

1. Initialize your Go module (if you haven't already):
    ```sh
    go mod init your_module_name
    ```

2. Get the `cutil` package:
    ```sh
    go get github.com/Seann-Moser/cutil
    ```

## Usage

Here's a brief overview of how to use the `cachec` package.

### Creating a Cache

```go
package main

import (
   "context"
   "fmt"
   "github.com/Seann-Moser/cutil/cachec"
   "github.com/patrickmn/go-cache"
   "time"
)

func main() {
   ctx := context.Background()
   // Create a new cache with a default expiration time of 5 minutes and cleanup interval of 10 minutes
   c := cachec.NewGoCache(cache.New(5*time.Minute, time.Minute), 5*time.Minute, "default")
   ctx = cachec.ContextWithCache(ctx, c)
   // Set a value in the cache with a key "foo" and a custom expiration time of 1 minute
   err := cachec.Set[string](ctx, "group-test", "foo", "bar")
   if err != nil {
      return
   }
   // Get the value associated with the key "foo"
   value, err := cachec.Get[string](ctx, "group-test", "foo")
   if err != nil {
      fmt.Println("Value not found")
   } else {
      fmt.Println("Found value:", value)
   }

   // Wait for 2 minutes
   time.Sleep(2 * time.Minute)
   
   // Try to get the value again after the expiration time
   value, err = cachec.Get[string](ctx, "group-test", "foo")
   if err != nil {
      fmt.Println("Value not found")
   } else {
      fmt.Println("Found value:", value)
   }
}
```


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -m 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

