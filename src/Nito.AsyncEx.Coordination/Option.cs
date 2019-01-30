namespace Nito.AsyncEx
{
    /// <summary>
    /// Represents an optional value,
    /// which can be Some value, representing the presence of a value;
    /// or None, representing the absence of a value.
    /// </summary>
    /// <typeparam name="TValue">
    /// The type of the value encapsulated by this <see cref="Option{TValue}"/>.
    /// </typeparam>
    public struct Option<TValue>
    {
        /// <summary>
        /// Initializes a new instance of <see cref="Option{TValue}"/>
        /// with the provided <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The value to be encapsulated.</param>
        private Option(TValue value)
        {
            Value = value;
            HasValue = true;
        }

        /// <summary>
        /// Gets whether this <see cref="Option{TValue}"/> contains a value.
        /// </summary>
        public bool HasValue { get; }

        /// <summary>
        /// Gets the <typeparamref name="TValue"/> contained by this <see cref="Option{TValue}"/>.
        /// </summary>
        private TValue Value { get; }

        /// <summary>
        /// Converts the provided <paramref name="value"/> to a <see cref="Option{TValue}"/>.
        /// </summary>
        /// <param name="value">
        /// The value to be contained by the resulting <see cref="Option{TValue}"/>.
        /// </param>
        public static implicit operator Option<TValue>(TValue value) => new Option<TValue>(value);

        /// <summary>
        /// Tries to get the value contained in this <see cref="Option{TValue}"/>.
        /// </summary>
        /// <param name="value">
        /// When this method returns, contains the <typeparamref name="TValue"/> value
        /// contained by this <see cref="Option{TValue}"/>, if it has a value;
        /// otherwise, the <c>default</c> value for the <typeparamref name="TValue"/> type.
        /// This parameter is passed uninitialized.
        /// </param>
        /// <returns>
        /// <c>true</c> if this <see cref="Option{TValue}"/> contains a value; otherwise, <c>false</c>.
        /// </returns>
        public bool TryGetValue(out TValue value)
        {
            if (!HasValue)
            {
                value = default(TValue);
                return false;
            }

            value = Value;
            return true;
        }
    }
}