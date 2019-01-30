using System;
using System.Collections.Generic;

namespace Nito.AsyncEx
{
    /// <summary>
    /// Extension methods for <see cref="Queue{T}"/>.
    /// </summary>
    internal static class QueueExtensions
    {
        /// <summary>
        /// Dequeues multiple items from the provided <paramref name="queue"/>.
        /// </summary>
        /// <typeparam name="T">The type of elements contained in the queue.</typeparam>
        /// <param name="queue">
        /// The <see cref="Queue{T}"/> where the elements should be dequeued from.
        /// </param>
        /// <param name="count">
        /// The maximum number of items to be returned in a single chunk.
        /// If the <paramref name="queue"/> currently has less items than the specified quantity,
        /// all items will be dequeued.
        /// </param>
        /// <returns>An array containing the dequeued items.</returns>
        public static T[] EagerDequeue<T>(this Queue<T> queue, int count)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            count = Math.Min(queue.Count, count);
            if (count <= 0) return Array.Empty<T>();

            var values = new T[count];

            for (var i = 0; i < count; i++)
            {
                values[i] = queue.Dequeue();
            }

            return values;
        }
    }
}