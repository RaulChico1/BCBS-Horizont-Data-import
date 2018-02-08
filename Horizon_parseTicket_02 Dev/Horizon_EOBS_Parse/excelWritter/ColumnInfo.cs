using System;
using System.Collections.Generic;
using System.Text;

namespace Horizon_EOBS_Parse.excelWritter
{
    internal class ColumnInfo
    {
        public int Width { get; set; }
        public int Index { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is ColumnInfo)
            {
                return (this.Index == ((ColumnInfo)obj).Index);
            }
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
