using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Linq2DynamoDb.DataContext.Caching.MemcacheD
{
    /// <summary>
    /// Implements a set of keys with a size limit 
    /// </summary>
    [Serializable]
    public class TableIndexList : IEnumerable<string>
    {
        private readonly LinkedList<string> _list;
        private readonly HashSet<string> _set; 
        private readonly int _maxCount;

        /// <summary>
        /// ctor
        /// </summary>
        public TableIndexList(int maxCount)
        {
            this._maxCount = maxCount;
            this._list = new LinkedList<string>();
            this._set = new HashSet<string>();
        }

        public int Count
        {
            get { return this._set.Count; }
        }

        /// <summary>
        /// Pushes a key to the set and returns the key, that was popped from another side, if any.
        /// </summary>
        public string Push(string indexKey)
        {
            if (this._set.Contains(indexKey))
            {
                return null;
            }

            string poppedKey = null;

            if (this._set.Count >= this._maxCount)
            {
                poppedKey = this._list.Last();
                this._set.Remove(poppedKey);
                this._list.RemoveLast();
            }

            if (this._set.Add(indexKey))
            {
                this._list.AddFirst(indexKey);
            }

            return poppedKey;
        }

        public void Remove(string indexKey)
        {
            if (this._set.Remove(indexKey))
            {
                // if HashSet contains this element, then we have to remove it from LinkedList as well.
                // This is a long yet rare operation.
                this._list.Remove(indexKey);
            }
        }

        public bool Contains(string indexKey)
        {
            return this._set.Contains(indexKey);
        }

        #region IEnumerable implementation

        public IEnumerator<string> GetEnumerator()
        {
            return this._list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this._list.GetEnumerator();
        }

        #endregion
    }
}
