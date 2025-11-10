# Этап сборки
FROM ubuntu:24.04 as builder

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y curl build-essential

# Установка Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --profile minimal -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Создание пустого проекта для кеширования зависимостей
WORKDIR /usr/src/app
RUN USER=root cargo new --bin hft
WORKDIR /usr/src/app/hft
COPY Cargo.lock Cargo.toml ./
RUN cargo build --release
RUN rm src/*.rs

# Копирование исходного кода и сборка проекта
COPY src ./src
RUN rm ./target/release/deps/hft*
RUN cargo build --release

# Этап запуска
FROM ubuntu:24.04

# Установка зависимостей для запуска
RUN apt-get update && apt-get install -y ca-certificates

# Копирование исполняемого файла из этапа сборки
COPY --from=builder /usr/src/app/hft/target/release/hft /usr/local/bin/hft

# Установка рабочего каталога
WORKDIR /app

# Запуск приложения
ENTRYPOINT ["/usr/local/bin/hft"]
