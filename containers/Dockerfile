FROM ubuntu:24.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        catch2 \
        cmake \
        ninja-build \
        python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake --preset asan-ubsan \
    && cmake --build --preset asan-ubsan --parallel 2

CMD ["ctest", "--preset", "asan-ubsan"]
