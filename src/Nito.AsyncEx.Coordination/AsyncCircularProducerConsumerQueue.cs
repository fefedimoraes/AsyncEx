using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx.Synchronous;

namespace Nito.AsyncEx
{
    /// <summary>
    /// An async-compatible producer/consumer queue that drops elements
    /// from the head of the queue whenever the quantity of elements
    /// is greater than a specified bounded capacity.
    /// </summary>
    /// <typeparam name="T">The type of elements contained in the queue.</typeparam>
    [DebuggerDisplay("Count = {_queue.Count}, MaxCount = {_maxCount}")]
    [DebuggerTypeProxy(typeof(AsyncCircularProducerConsumerQueue<>.DebugView))]
    public sealed class AsyncCircularProducerConsumerQueue<T>
    {
        /// <summary>
        /// The underlying queue.
        /// </summary>
        private readonly Queue<T> _queue;

        /// <summary>
        /// The maximum number of elements allowed in the queue.
        /// </summary>
        private readonly int _maxCount;

        /// <summary>
        /// The mutual-exclusion lock protecting <see cref="_queue"/> and <see cref="_isCompleted"/>.
        /// </summary>
        private readonly AsyncLock _mutex;

        /// <summary>
        /// A condition variable that is signaled when the queue is completed or not empty.
        /// </summary>
        private readonly AsyncConditionVariable _completedOrNotEmpty;

        /// <summary>
        /// Whether this producer/consumer queue has been marked complete for adding.
        /// </summary>
        private bool _isCompleted;

        /// <summary>
        /// Creates a new async-compatible producer/consumer queue
        /// with the specified initial elements and a maximum element count.
        /// </summary>
        /// <param name="collection">
        /// The initial elements to place in the queue.
        /// This may be <c>null</c> to start with an empty collection.
        /// </param>
        /// <param name="maxCount">
        /// The maximum element count. This must be greater than zero,
        /// and greater than or equal to the number of elements in <paramref name="collection"/>.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="maxCount"/> is less than or equal to zero.
        /// </exception>
        /// <exception cref="ArgumentException">
        /// <paramref name="maxCount"/> is less than the number of elements in the <paramref name="collection"/>.
        /// </exception>
        public AsyncCircularProducerConsumerQueue(IEnumerable<T> collection, int maxCount)
        {
            if (maxCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxCount), "The maximum count must be greater than zero.");

            _queue = collection == null ? new Queue<T>() : new Queue<T>(collection);

            if (maxCount < _queue.Count)
                throw new ArgumentException("The maximum count cannot be less than the number of elements in the collection.", nameof(maxCount));

            _maxCount = maxCount;

            _mutex = new AsyncLock();
            _completedOrNotEmpty = new AsyncConditionVariable(_mutex);
        }

        /// <summary>
        /// Creates a new async-compatible producer/consumer queue
        /// with the specified initial elements.
        /// </summary>
        /// <param name="collection">
        /// The initial elements to place in the queue.
        /// This may be <c>null</c> to start with an empty collection.
        /// </param>
        public AsyncCircularProducerConsumerQueue(IEnumerable<T> collection)
            : this(collection, int.MaxValue)
        {
        }

        /// <summary>
        /// Creates a new async-compatible producer/consumer queue
        /// with a maximum element count.
        /// </summary>
        /// <param name="maxCount">
        /// The maximum element count. This must be greater than zero.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <paramref name="maxCount"/> is less than or equal to zero.
        /// </exception>
        public AsyncCircularProducerConsumerQueue(int maxCount)
            : this(null, maxCount)
        {
        }

        /// <summary>
        /// Creates a new async-compatible producer/consumer queue.
        /// </summary>
        public AsyncCircularProducerConsumerQueue()
            : this(null, int.MaxValue)
        {
        }

        /// <summary>
        /// Whether the queue is empty.
        /// This property assumes that the <see cref="_mutex"/> is already held.
        /// </summary>
        private bool IsEmpty => _queue.Count == 0;

        /// <summary>
        /// Whether the queue is full.
        /// This property assumes that the <see cref="_mutex"/> is already held.
        /// </summary>
        private bool IsFull => _queue.Count == _maxCount;

        /// <summary>
        /// Marks this producer/consumer queue as complete for adding.
        /// </summary>
        public void CompleteAdding()
        {
            using (_mutex.Lock())
            {
                _isCompleted = true;
                _completedOrNotEmpty.NotifyAll();
            }
        }

        /// <summary>
        /// Attempts to enqueue an <paramref name="item"/> to this producer/consumer queue.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains
        /// <c>true</c> if this producer/consume queue has not been marked complete
        /// and the provided <paramref name="item"/> could be added; otherwise, <c>false</c>.
        /// </returns>
        public Task<bool> TryEnqueueAsync(T item) =>
            TryEnqueueAsync(item, false);

        /// <summary>
        /// Enqueues an <paramref name="item"/> to this producer/consumer queue.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <returns>
        /// A <see cref="Task"/> that completes when the <paramref name="item"/>
        /// is added to this producer/consumer queue.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding.
        /// </exception>
        public Task EnqueueAsync(T item) =>
            EnqueueAsync(item, false);

        /// <summary>
        /// Attempts to enqueue an <paramref name="item"/> to this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <returns>
        /// <c>true</c> if this producer/consume queue has not been marked complete
        /// and the provided <paramref name="item"/> could be added; otherwise, <c>false</c>.
        /// </returns>
        public bool TryEnqueue(T item) =>
            TryEnqueueAsync(item, true).WaitAndUnwrapException();

        /// <summary>
        /// Enqueues an <paramref name="item"/> to this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding.
        /// </exception>
        public void Enqueue(T item) =>
            EnqueueAsync(item, true).WaitAndUnwrapException();

        /// <summary>
        /// Asynchronously waits until an item is available to dequeue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the asynchronous wait.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains
        /// <c>false</c> if this producer/consumer queue
        /// has completed adding and there are no more items;
        /// otherwise, <c>true</c>.
        /// </returns>
        public Task<bool> OutputAvailableAsync(CancellationToken cancellationToken) =>
            OutputAvailableAsync(cancellationToken, false);

        /// <summary>
        /// Asynchronously waits until an item is available to dequeue.
        /// </summary>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains
        /// <c>false</c> if this producer/consumer queue
        /// has completed adding and there are no more items;
        /// otherwise, <c>true</c>.
        /// </returns>
        public Task<bool> OutputAvailableAsync() =>
            OutputAvailableAsync(CancellationToken.None);

        /// <summary>
        /// Synchronously waits until an item is available to dequeue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the synchronous wait.
        /// </param>
        /// <returns>
        /// <c>false</c> if this producer/consumer queue
        /// has completed adding and there are no more items;
        /// otherwise, <c>true</c>.
        /// </returns>
        public bool OutputAvailable(CancellationToken cancellationToken) =>
            OutputAvailableAsync(cancellationToken, true).WaitAndUnwrapException();

        /// <summary>
        /// Synchronously waits until an item is available to dequeue.
        /// This method may block the calling thread.
        /// </summary>
        /// <returns>
        /// <c>false</c> if this producer/consumer queue
        /// has completed adding and there are no more items;
        /// otherwise, <c>true</c>.
        /// </returns>
        public bool OutputAvailable() =>
            OutputAvailable(CancellationToken.None);

        /// <summary>
        /// Provides a (synchronous) consuming enumerable for items in this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the synchronous enumeration.
        /// </param>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> that removes and returns items from this producer/consumer queue.
        /// </returns>
        public IEnumerable<T> GetConsumingEnumerable(CancellationToken cancellationToken)
        {
            while (true)
            {
                var option = TryDequeueAsync(cancellationToken, true).WaitAndUnwrapException();
                if (!option.TryGetValue(out var value))
                    yield break;

                yield return value;
            }
        }

        /// <summary>
        /// Provides a (synchronous) consuming enumerable for items in this producer/consumer queue.
        /// </summary>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> that removes and returns items from this producer/consumer queue.
        /// </returns>
        public IEnumerable<T> GetConsumingEnumerable() =>
            GetConsumingEnumerable(CancellationToken.None);

        /// <summary>
        /// Attempts to dequeue an item from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain the dequeued item, if any item was available.
        /// </returns>
        public Task<Option<T>> TryDequeueAsync(CancellationToken cancellationToken) =>
            TryDequeueAsync(cancellationToken, false);

        /// <summary>
        /// Attempts to dequeue an item from this producer/consumer queue.
        /// </summary>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain the dequeued item, if any item was available.
        /// </returns>
        public Task<Option<T>> TryDequeueAsync() =>
            TryDequeueAsync(CancellationToken.None);

        /// <summary>
        /// Attempts to dequeue multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain an array with the dequeued items.
        /// </returns>
        public Task<Option<T[]>> TryDequeueAsync(int count, CancellationToken cancellationToken) =>
            TryDequeueAsync(cancellationToken, count, false);

        /// <summary>
        /// Attempts to dequeue multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain an array with the dequeued items.
        /// </returns>
        public Task<Option<T[]>> TryDequeueAsync(int count) =>
            TryDequeueAsync(count, CancellationToken.None);

        /// <summary>
        /// Dequeues an item from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains the dequeued item.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public Task<T> DequeueAsync(CancellationToken cancellationToken) =>
            DequeueAsync(cancellationToken, false);

        /// <summary>
        /// Dequeues an item from this producer/consumer queue.
        /// </summary>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains the dequeued item.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public Task<T> DequeueAsync() =>
            DequeueAsync(CancellationToken.None);

        /// <summary>
        /// Dequeues multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains an array the with the dequeued items.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public Task<T[]> DequeueAsync(int count, CancellationToken cancellationToken) =>
            DequeueAsync(cancellationToken, count, false);

        /// <summary>
        /// Dequeues multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains an array the with the dequeued items.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public Task<T[]> DequeueAsync(int count) =>
            DequeueAsync(count, CancellationToken.None);

        /// <summary>
        /// Dequeues an item from this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>The dequeued item.</returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public T Dequeue(CancellationToken cancellationToken) =>
            DequeueAsync(cancellationToken, true).WaitAndUnwrapException();

        /// <summary>
        /// Dequeues an item from this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <returns>The dequeued item.</returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public T Dequeue() =>
            Dequeue(CancellationToken.None);

        /// <summary>
        /// Dequeues multiple items from this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <returns>An array the with the dequeued items..</returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public T[] Dequeue(int count, CancellationToken cancellationToken) =>
            DequeueAsync(cancellationToken, count, true).WaitAndUnwrapException();

        /// <summary>
        /// Dequeues multiple items from this producer/consumer queue.
        /// This method may block the calling thread.
        /// </summary>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <returns>An array the with the dequeued items.</returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        public T[] Dequeue(int count) =>
            Dequeue(count, CancellationToken.None);

        /// <summary>
        /// Attempts to enqueue an <paramref name="item"/> to this producer/consumer queue.
        /// </summary>
        /// <param name="item">
        /// The item to enqueue.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains
        /// <c>true</c> if this producer/consume queue has not been marked complete
        /// and the provided <paramref name="item"/> could be added; otherwise, <c>false</c>.
        /// </returns>
        private async Task<bool> TryEnqueueAsync(T item, bool sync)
        {
            using (sync ? _mutex.Lock() : await _mutex.LockAsync().ConfigureAwait(false))
            {
                // If the queue has been marked complete, then abort.
                if (_isCompleted)
                    return false;

                if (IsFull) _queue.Dequeue();

                _queue.Enqueue(item);
                _completedOrNotEmpty.Notify();
                return true;
            }
        }

        /// <summary>
        /// Attempts to enqueue an <paramref name="item"/> to this producer/consumer queue.
        /// </summary>
        /// <param name="item">
        /// The item to enqueue.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task"/> that completes when the <paramref name="item"/>
        /// is added to this producer/consumer queue.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding.
        /// </exception>
        private async Task EnqueueAsync(T item, bool sync)
        {
            if (!await TryEnqueueAsync(item, sync).ConfigureAwait(false))
                throw new InvalidOperationException("Enqueue failed; the producer/consumer queue has completed adding.");
        }

        /// <summary>
        /// Waits until an item is available to dequeue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the asynchronous wait.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains
        /// <c>false</c> if this producer/consumer queue has completed adding and there are no more items;
        /// otherwise, <c>true</c>.
        /// </returns>
        private async Task<bool> OutputAvailableAsync(CancellationToken cancellationToken, bool sync)
        {
            using (sync ? _mutex.Lock() : await _mutex.LockAsync().ConfigureAwait(false))
            {
                while (IsEmpty && !_isCompleted)
                {
                    if (sync)
                        _completedOrNotEmpty.Wait(cancellationToken);
                    else
                        await _completedOrNotEmpty.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                return !IsEmpty;
            }
        }

        /// <summary>
        /// Attempts to dequeue an item from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain the dequeued items.
        /// </returns>
        private async Task<Option<T>> TryDequeueAsync(CancellationToken cancellationToken, bool sync)
        {
            using (sync ? _mutex.Lock() : await _mutex.LockAsync().ConfigureAwait(false))
            {
                while (IsEmpty && !_isCompleted)
                {
                    if (sync)
                        _completedOrNotEmpty.Wait(cancellationToken);
                    else
                        await _completedOrNotEmpty.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                return _isCompleted && IsEmpty ? default(Option<T>) : _queue.Dequeue();
            }
        }

        /// <summary>
        /// Attempts to dequeue multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed,
        /// contains a <see cref="Option{TValue}"/>
        /// that may contain an array with the dequeued items.
        /// </returns>
        private async Task<Option<T[]>> TryDequeueAsync(CancellationToken cancellationToken, int count, bool sync)
        {
            using (sync ? _mutex.Lock() : await _mutex.LockAsync().ConfigureAwait(false))
            {
                while (IsEmpty && !_isCompleted)
                {
                    if (sync)
                        _completedOrNotEmpty.Wait(cancellationToken);
                    else
                        await _completedOrNotEmpty.WaitAsync(cancellationToken).ConfigureAwait(false);
                }

                return _isCompleted && IsEmpty ? default(Option<T[]>) : _queue.EagerDequeue(count);
            }
        }

        /// <summary>
        /// Dequeues an item from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains the dequeued item.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        private async Task<T> DequeueAsync(CancellationToken cancellationToken, bool sync) =>
            (await TryDequeueAsync(cancellationToken, sync).ConfigureAwait(false)).TryGetValue(out var value)
                ? value
                : throw new InvalidOperationException("Dequeue failed; the producer/consumer queue has completed adding and is empty.");

        /// <summary>
        /// Dequeues multiple items from this producer/consumer queue.
        /// </summary>
        /// <param name="cancellationToken">
        /// A <see cref="CancellationToken"/> that can be used to abort the dequeue operation.
        /// </param>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the queue currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <param name="sync">
        /// Whether to run this method synchronously (<c>true</c>) or asynchronously (<c>false</c>).
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> that, when completed, contains the array with the dequeued items.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// This producer/consumer queue has been marked complete for adding and is empty.
        /// </exception>
        private async Task<T[]> DequeueAsync(CancellationToken cancellationToken, int count, bool sync) =>
            (await TryDequeueAsync(cancellationToken, count, sync).ConfigureAwait(false)).TryGetValue(out var value)
                ? value
                : throw new InvalidOperationException("Dequeue failed; the producer/consumer queue has completed adding and is empty.");

        [DebuggerNonUserCode]
        internal sealed class DebugView
        {
            private readonly AsyncCircularProducerConsumerQueue<T> _queue;

            public DebugView(AsyncCircularProducerConsumerQueue<T> queue) => _queue = queue;

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public T[] Items => _queue._queue.ToArray();
        }
    }
}
