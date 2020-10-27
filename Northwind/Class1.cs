using System;
using System.Collections.Generic;
using System.Text;

namespace Northwind
{
    public class External
    {
        public static string GetVal()
        {
            return DateTime.UtcNow.ToString();
        }
    }
}
