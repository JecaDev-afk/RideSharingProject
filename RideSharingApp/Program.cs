using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;

// ---------------------------------------------------------
// 1. НАЛАШТУВАННЯ ТА ЗАПУСК (BOOTSTRAP)
// ---------------------------------------------------------
var builder = WebApplication.CreateBuilder(args);

// Додаємо сервіси до контейнера
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddControllers();
builder.Services.AddSignalR(); // Для Real-Time комунікації

// Реєстрація Singleton сервісів (імітація баз даних в пам'яті)
builder.Services.AddSingleton<LocationStore>(); // Імітація Redis (Геодані)
builder.Services.AddSingleton<TripRepository>(); // Імітація PostgreSQL (Дані поїздок)
builder.Services.AddSingleton<MatchingQueue>(); // Імітація Kafka (Черга повідомлень)

// Реєстрація фонового воркера (Matching Engine)
builder.Services.AddHostedService<MatchingEngineWorker>();

var app = builder.Build();

// Налаштування HTTP пайплайну
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Додаємо підтримку статичних файлів (для index.html)
app.UseDefaultFiles();
app.UseStaticFiles();

app.UseHttpsRedirection();
app.MapControllers();
app.MapHub<RideHub>("/rideHub"); // WebSocket точка входу

Console.WriteLine("--> Ride-Sharing Service запущено.");
Console.WriteLine("--> Відкрийте https://localhost:{PORT}/index.html для тестування.");

app.Run();

// ---------------------------------------------------------
// 2. МОДЕЛІ ДАНИХ (DOMAIN MODELS)
// ---------------------------------------------------------

public record Location(double Lat, double Lon);

public class Driver
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public Location CurrentLocation { get; set; } = new(0, 0);
    public bool IsAvailable { get; set; } = true;
}

public class TripRequest
{
    public string PassengerId { get; set; } = string.Empty;
    public Location PickupLocation { get; set; } = new(0, 0);
}

public class Trip
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string PassengerId { get; set; } = string.Empty;
    public string? DriverId { get; set; }
    public string Status { get; set; } = "PENDING"; // Статуси: PENDING, MATCHED, COMPLETED
    public Location Pickup { get; set; } = new(0, 0);
}

// ---------------------------------------------------------
// 3. ІМІТАЦІЯ ІНФРАСТРУКТУРИ (IN-MEMORY STORES)
// ---------------------------------------------------------

// Імітація Redis Geo: Зберігає координати водіїв
public class LocationStore
{
    private readonly ConcurrentDictionary<string, Driver> _drivers = new();

    public void UpdateDriverLocation(string driverId, double lat, double lon)
    {
        _drivers.AddOrUpdate(driverId,
            new Driver { Id = driverId, CurrentLocation = new Location(lat, lon) },
            (key, existing) =>
            {
                existing.CurrentLocation = new Location(lat, lon);
                return existing;
            });
    }

    public List<Driver> GetAvailableDrivers() => _drivers.Values.Where(d => d.IsAvailable).ToList();

    public void SetDriverBusy(string driverId)
    {
        if (_drivers.TryGetValue(driverId, out var driver))
        {
            driver.IsAvailable = false;
        }
    }
}

// Імітація PostgreSQL: Зберігає стан поїздок
public class TripRepository
{
    private readonly ConcurrentDictionary<string, Trip> _trips = new();

    public void Save(Trip trip) => _trips[trip.Id] = trip;
    public Trip? Get(string id) => _trips.TryGetValue(id, out var trip) ? trip : null;
}

// Імітація Kafka: Асинхронна черга для передачі подій
public class MatchingQueue
{
    private readonly Channel<string> _channel = Channel.CreateUnbounded<string>();

    public async Task PublishTripRequestedAsync(string tripId)
    {
        await _channel.Writer.WriteAsync(tripId);
    }

    public IAsyncEnumerable<string> ReadAllAsync(CancellationToken ct) => _channel.Reader.ReadAllAsync(ct);
}

// ---------------------------------------------------------
// 4. API КОНТРОЛЕРИ (API GATEWAY)
// ---------------------------------------------------------

[ApiController]
[Route("api/[controller]")]
public class TripController : ControllerBase
{
    private readonly TripRepository _tripRepo;
    private readonly MatchingQueue _queue;

    public TripController(TripRepository tripRepo, MatchingQueue queue)
    {
        _tripRepo = tripRepo;
        _queue = queue;
    }

    // Пасажир створює запит на поїздку
    [HttpPost("request")]
    public async Task<IActionResult> RequestRide([FromBody] TripRequest request)
    {
        // 1. Створення запису в БД (Pending)
        var trip = new Trip
        {
            PassengerId = request.PassengerId,
            Pickup = request.PickupLocation,
            Status = "PENDING"
        };

        _tripRepo.Save(trip);

        // 2. Публікація події в чергу (Асинхронна обробка для масштабованості)
        await _queue.PublishTripRequestedAsync(trip.Id);

        // Повертаємо 202 Accepted миттєво, не чекаючи пошуку водія
        return Accepted(new { TripId = trip.Id, Status = "Searching for driver..." });
    }

    [HttpGet("{id}")]
    public IActionResult GetTrip(string id)
    {
        var trip = _tripRepo.Get(id);
        return trip != null ? Ok(trip) : NotFound();
    }
}

[ApiController]
[Route("api/[controller]")]
public class DriverController : ControllerBase
{
    private readonly LocationStore _locationStore;
    public DriverController(LocationStore locationStore) => _locationStore = locationStore;

    // Водій оновлює свої координати
    [HttpPost("update-location")]
    public IActionResult Update([FromBody] Driver driver)
    {
        _locationStore.UpdateDriverLocation(driver.Id, driver.CurrentLocation.Lat, driver.CurrentLocation.Lon);
        return Ok();
    }
}

// ---------------------------------------------------------
// 5. SIGNALR HUB (REAL-TIME UPDATES)
// ---------------------------------------------------------

public class RideHub : Hub
{
    // Клієнт підписується на оновлення конкретної поїздки
    public async Task JoinRideGroup(string tripId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, $"trip_{tripId}");
        await Clients.Caller.SendAsync("Log", $"Підписано на оновлення для {tripId}");
    }
}

// ---------------------------------------------------------
// 6. BACKGROUND WORKER (MATCHING ENGINE)
// ---------------------------------------------------------

public class MatchingEngineWorker : BackgroundService
{
    private readonly MatchingQueue _queue;
    private readonly LocationStore _locationStore;
    private readonly TripRepository _tripRepo;
    private readonly IHubContext<RideHub> _hubContext;
    private readonly ILogger<MatchingEngineWorker> _logger;

    public MatchingEngineWorker(
        MatchingQueue queue,
        LocationStore locationStore,
        TripRepository tripRepo,
        IHubContext<RideHub> hubContext,
        ILogger<MatchingEngineWorker> logger)
    {
        _queue = queue;
        _locationStore = locationStore;
        _tripRepo = tripRepo;
        _hubContext = hubContext;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(">>> Matching Engine (Worker) запущено і очікує замовлення...");

        // Слухаємо чергу повідомлень (як Kafka Consumer)
        await foreach (var tripId in _queue.ReadAllAsync(stoppingToken))
        {
            try
            {
                await ProcessMatchingAsync(tripId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Помилка обробки поїздки {TripId}", tripId);
            }
        }
    }

    private async Task ProcessMatchingAsync(string tripId)
    {
        _logger.LogInformation(">>> Обробка матчингу для: {TripId}", tripId);

        // Емуляція складної логіки пошуку (затримка 3 секунди)
        await Task.Delay(3000);

        var trip = _tripRepo.Get(tripId);
        if (trip == null) return;

        // Пошук найближчого водія (спрощена Евклідова відстань)
        var drivers = _locationStore.GetAvailableDrivers();
        var bestDriver = drivers.MinBy(d =>
            Math.Pow(d.CurrentLocation.Lat - trip.Pickup.Lat, 2) +
            Math.Pow(d.CurrentLocation.Lon - trip.Pickup.Lon, 2));

        if (bestDriver != null)
        {
            // Атомарне оновлення стану
            trip.DriverId = bestDriver.Id;
            trip.Status = "MATCHED";
            _tripRepo.Save(trip);

            // Позначаємо водія зайнятим
            _locationStore.SetDriverBusy(bestDriver.Id);

            _logger.LogInformation(">>> УСПІХ: Поїздку {TripId} призначено водію {DriverId}", tripId, bestDriver.Id);

            // ВІДПРАВКА REAL-TIME ПОВІДОМЛЕННЯ КЛІЄНТУ (PUSH)
            await _hubContext.Clients.Group($"trip_{tripId}")
                .SendAsync("RideUpdate", new { Status = "MATCHED", DriverId = bestDriver.Id });
        }
        else
        {
            _logger.LogWarning(">>> НЕВДАЧА: Водіїв не знайдено для {TripId}", tripId);
            // Тут можна було б реалізувати логіку повторної спроби (Retry)
        }
    }
}