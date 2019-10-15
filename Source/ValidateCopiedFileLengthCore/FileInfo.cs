using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ValidateCopiedFileLengthCore
{
    public class FileItemInfo
    {
        public string Name { get; set; }
        public long? Length { get; set; }
        public DateTime? LastModifiedUtc { get; set; }

        public override string ToString()
        {
            return string.Join(", ", Name, Length, LastModifiedUtc);
        }
    }
}
