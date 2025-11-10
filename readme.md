strategy:
- нужно проверять, не закрылся ли канал?
- в идеале нам нужна MPSC

processor:
- нужно проверять, не закрылся ли канал?
- в идеале нам нужна MPMC - чтобы разные processors могла обрабатывать сообщения из одной очереди (для балансировки). тогда у одного processor могут быть несколько очередей для мониторинга.

TODO:
- сделать балансировку для stage1, если в stage1_rules в processors могут быть несколько значений
- роутинг к стратегиям для правильной очередности
- добавить треды для стейджей?

commands:

```bash
cargo build --profile profiling
samply record ./target/profiling/hft /Users/railka/lab/rust/hft/configs/baseline.json --mode queue
```

## Stage threads

если мы вводим новые потоки для stage1 и stage2, какие должны быть очереди между всеми потоками?

возможно мы возьмем SPSC для producers, processors, strategies.
- producer будет иметь только Sender
- processor будет иметь Receiver<Message>, Sender<HandledMessage>
- strategy будет иметь только Receiver

у них все операции блокирующие через spin loop.

stage1 будет принимать сообщения от producer и отправлять processor на основе конфига: если processor один, то ему, если несколько, то тому, который менее загружен. Если processor загружен, откладываем во внутреннюю очередь.

stage2 будет принимать сообщения от processor, валидировать сортировку/очередность, отправлять strategy.

для stage2 какая будет матрица?
- для каждого producer будет создана SPSC. например 4.
- для каждого strategy тоже свой SPSC: 3.
- stage2 хранит producers: Vec<Receiver>, strategies: Vec<Sender>
- мониторит все `Vec<Receiver>` в цикле
