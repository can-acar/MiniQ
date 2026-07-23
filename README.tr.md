# MiniQ

**[English](README.md) · Türkçe**

Doğrudan **native [`RabbitMQ.Client`](https://www.nuget.org/packages/RabbitMQ.Client)** üzerine kurulmuş, hafif ve görüşlü bir mesajlaşma araç seti — MassTransit yok, ağır soyutlama katmanı yok. MiniQ; küçük ve akıcı bir DI builder'ının arkasında tipli publisher/consumer'lar, bağlantı paylaşımı, kanal havuzlama, quorum kuyrukları ve otomatik retry / dead-letter topolojisi sunar.

Bu bir **ince katmandır, framework değildir**: altdaki `IChannel` semantiğine tam erişimin korunur ve MiniQ'nun tanımladığı her şey, management UI'da inceleyebileceğin sade RabbitMQ topolojisidir.

## Neden MiniQ

- **Native client, sıfır sihir.** `RabbitMQ.Client` 7.x üzerine kurulu. Message-bus runtime'ı yok, reflection ağırlıklı konvansiyonlar yok.
- **Tipli publish/consume.** `IRabbitMqPublisher<T>` ve `IMessageHandler<T>` — routing kayıt anında sabitlenir.
- **Varsayılan olarak dayanıklı.** Otomatik kurtarılan paylaşımlı bağlantı, publisher confirms'lı sınırlı publisher kanal havuzu, üstel publish retry'ı.
- **Güvenilir teslimat.** Consumer başına prefetch, quorum kuyrukları, gecikmeli retry kuyrukları (TTL + dead-letter) ve poison mesajlar için bir dead-letter kuyruğu.
- **Bildirimsel topoloji.** Exchange'ler, kuyruklar, DLQ ve retry kuyrukları başlangıçta hosted bir initializer tarafından bir kez bildirilir.
- **Küçük yüzey.** Tek bir `AddRabbitMq(...)` çağrısı her şeyi `Microsoft.Extensions.DependencyInjection`'a bağlar.

## Kurulum

```bash
dotnet add package MiniQ
```

`net8.0` ve `net10.0` hedefler.

## Hızlı başlangıç

```csharp
using MiniQ;
using Microsoft.Extensions.DependencyInjection;

public sealed record OrderCreated(string OrderId, decimal Amount);

public sealed class OrderCreatedHandler : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated message, CancellationToken ct = default)
    {
        // ... işini yap; retry / dead-letter tetiklemek için exception fırlat
        return Task.CompletedTask;
    }
}
```

Host'unda kaydet:

```csharp
builder.Services
    .AddOptions<RabbitMqOptions>()
    .Configure(o => o.Uri = "amqp://localhost:5672");

builder.Services.AddRabbitMq(rabbit =>
{
    // Paylaşılan bir exchange/routing-key'i bir kez tanımla, adıyla referans ver.
    rabbit.AddEndpoint("orders", e =>
    {
        e.Exchange   = "sample.orders";
        e.RoutingKey = "order.created";
    });

    rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
    {
        c.Queue                  = "sample.orders";
        c.DeadLetterExchange     = "sample.orders.dlx";
        c.DeadLetterQueue        = "sample.orders.dlq";
        c.PrefetchCount          = 16;
        c.MaxRetries             = 3;
        c.RetryDelayMilliseconds = 5000;   // gecikmeli retry kuyruğu "sample.orders.retry" olarak türetilir
    });

    rabbit.AddPublisher<OrderCreated>("orders");
});
```

Herhangi bir yerden publish et:

```csharp
public class OrderService(IRabbitMqPublisher<OrderCreated> publisher)
{
    public Task PlaceAsync(OrderCreated order) =>
        publisher.PublishAsync(order, correlationId: order.OrderId);
}
```

## Yapılandırma

`RabbitMqOptions`'ı `RabbitMq` bölümünden bağla (ya da inline yapılandır):

| Özellik | Varsayılan | Açıklama |
| --- | --- | --- |
| `Uri` | `amqp://localhost:5672` | Birincil AMQP endpoint'i |
| `UserName` / `Password` | *(URI'den)* | Opsiyonel kimlik bilgisi override'ları |
| `VirtualHost` | `/` | Virtual host |
| `Clusters` | *(boş)* | `;` ile ayrılmış failover node'ları |
| `ClientName` | `miniq` | Management UI'daki bağlantı adı |
| `PublishMaxRetries` | `3` | Başarısız olmadan önceki publish denemeleri |
| `RetryBaseDelayMilliseconds` | `500` | Üstel publish backoff için taban |
| `HeartbeatSeconds` | `30` | İstenen heartbeat |

Consumer başına ayarlar `RabbitMqConsumerOptions` üzerindedir (`Queue`, `PrefetchCount`, `QueueType` = `Quorum`/`Classic`, `DeliveryLimit`, `DispatchConcurrency`, `MaxRetries`, `RetryDelayMilliseconds`, dead-letter adları).

## Nasıl çalışır

```
 Publisher                                              Consumer
 ─────────                                              ────────
 IRabbitMqPublisher<T>                                  RabbitMqConsumerBackgroundService<T>
        │                                                        │  BasicQos(prefetch)
        ▼                                                        ▼
 RabbitMqSender ── rent ──► RabbitMqChannelPool          deserialize → IMessageHandler<T> (scoped)
   (JSON, confirms,          (bounded, publisher              │
    exp. retry)               confirms)                       ├── ok ──────────► ack
        │                          │                          │
        └──────────────┬──────────┘                          └── throws ──► retry queue (TTL)
                       ▼                                                     └► exhausted → DLQ
              IRabbitMqConnection  (shared, auto-recovering)
                       │
                       ▼
                   RabbitMQ broker
```

Başlangıçta `RabbitMqTopologyInitializer` (bir `IHostedService`) kayıtlı her exchange, kuyruk, dead-letter exchange/kuyruk ve gecikmeli retry kuyruğunu bildirir. Sıcak yolda:

- **Publish**, payload'ı bir kez JSON'a serialize eder, havuzdan confirm'li bir kanal kiralar ve geçici hatalarda üstel retry ile kalıcı (persistent) olarak publish eder.
- **Consume**, sınırlı bir prefetch ile çeker, her mesajı scoped bir `IMessageHandler<T>`'a dağıtır ve başarıda ack'ler. Hatada mesajı TTL tabanlı bir retry kuyruğuna iletir (`x-retry-count` header'ını artırarak); `MaxRetries` tükenince mesaj dead-letter edilir.
- Kanalı kapanıp otomatik kurtarılamayan bir consumer, arka plan health check'inde aboneliğini yeniden kurar; böylece asla sessizce ölmez.

### Teslimat garantileri

MiniQ **at-least-once**'tır. Retry'lar ve atomik olmayan *retry'e-ilet-sonra-ack* adımı bir mesajı birden fazla teslim edebilir; publisher confirms yalnızca broker'ın mesajı aldığını kanıtlar — routing'i ya da exactly-once işlemeyi değil. **Handler'larını idempotent yap.** Route edilemeyen mesajların düşmek yerine gün yüzüne çıkmasını istiyorsan `RabbitMqPublisherOptions.Mandatory = true` ayarla.

### Effectively-once (idempotent consumer'lar)

Exactly-once *delivery* imkânsızdır; effectively-once *processing* yalnızca sink'te vardır. MiniQ sana sihirli bir garanti değil, seam (bağlantı noktası) verir. Bir consumer'ı `Idempotent` işaretle ve bir `IIdempotencyStore` kaydet; her teslimat o zaman dağıtımdan önce (`MessageId` ile) kontrol edilir ve başarıdan sonra kaydedilir:

```csharp
rabbit.UseIdempotencyStore<SqlInboxStore>();   // varsayılan olarak scoped

rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
{
    c.Queue = "sample.orders";
    c.Idempotent = true;
});
```

Store, **handler ile aynı DI scope'undan** çözülür; doğru implementasyonu mümkün kılan şey budur — **transactional inbox pattern**: store'un, işlenen `MessageId`'yi handler'ın iş yazımlarıyla *aynı veritabanı transaction'ında* kaydeder. Böylece kısmi bir hata, yan etkiyi commit'lenmiş ama id'yi kaydedilmemiş (ya da tersini) asla bırakamaz.

- `NoOpIdempotencyStore` varsayılandır — dedup yok, düz at-least-once.
- `InMemoryIdempotencyStore` **yalnızca geliştirme ve testler içindir**: kalıcı değildir, instance'lar arasında paylaşılmaz ve sınırsız büyür. Prod'da asla kullanma.
- Ön-kontrol + işaretleme iki ayrı işlemdir; yani bu bir kilit değil, best-effort bir filtredir. Pencereyi tamamen kapatan tek şey, id'yi handler'ın kendi işiyle transactional olarak yazan bir store'dur.

### Retry stratejileri

Başarısız bir handler, consumer başına `RetryStrategy` ile ayarlanan iki stratejiden biriyle yeniden denenir:

| | `Republish` (varsayılan) | `BrokerDeadLetter` |
| --- | --- | --- |
| Nasıl | Consumer retry kuyruğuna republish eder, sonra orijinali ack'ler | Consumer `nack`'ler; broker retry kuyruğuna dead-letter eder |
| Atomiklik | İki adım — arada bir crash mesajı çoğaltabilir | Sıcak hata yolunda tek atomik `nack` — dual-write yok |
| Retry sayımı | Consumer'ın artırdığı `x-retry-count` header'ı | Broker'ın `x-death` header'ı |
| Tükeniş | `nack(requeue: false)` → dead-letter kuyruğu | Dead-letter kuyruğuna explicit publish (nadir, yalnızca poison) |

```csharp
rabbit.AddConsumer<OrderCreated, OrderCreatedHandler>("orders", c =>
{
    c.Queue = "sample.orders";
    c.RetryStrategy = RetryStrategy.BrokerDeadLetter;
    c.DeadLetterQueue = "sample.orders.dlq";   // tükenmiş mesajların düştüğü yer
    c.MaxRetries = 3;
    c.RetryDelayMilliseconds = 5000;
});
```

`BrokerDeadLetter`, sıcak hata yolundaki republish dual-write'ını kaldırır; geriye kalan tek publish, nadir olan tükenmiş-mesajı-DLQ'ya adımıdır. Her iki strateji de sabit retry gecikmesi kullanır. Dead-letter kuyruğu yapılandırılmamışsa, tükenmiş mesajlar düşürülür (bir `nack` retry kuyruğuna geri döngü yapardı).

## Kaynaktan derleme

```bash
dotnet build
dotnet test
```

Birim testleri DI builder kayıtlarını ve kanal havuzu yaşam döngüsünü kapsar; çalışan bir broker **gerektirmez**. Örnekleri denemek için RabbitMQ'ya ihtiyacın var:

```bash
docker run -d --name miniq-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

İki örnek dahildir:

```bash
# Uçtan uca: tek kuyruk üzerinde bir producer ve bir consumer, retry + dead-letter topolojisiyle
dotnet run --project samples/MiniQ.Sample

# Yalnızca publisher: exchange'i bildirir, birkaç mesaj publish eder, sonra çıkar
dotnet run --project samples/MiniQ.Publisher.Sample
```

## Lisans

[MIT](LICENSE) © can-acar
