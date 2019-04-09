using System;
using System.Collections.Generic;
using System.Text;

namespace Horizon_EOBS_Parse
{
    class Field
    {
        private string _name;
        private int _start;
        private int _length;
        private string _value;

        public int Length
        {
            get { return _length; }
            set { _length = value; }
        }

        public int Start
        {
            get { return _start; }
            set { _start = value; }
        }

        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }

        public string Value
        {
            get { return _value; }
            set { _value = value; }
        }
    }
}
