using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace OsmEveryDay
{
    class Program
    {
        const int DEFAULT_ROWS = 1000;

        public class RowImportModel : IComparable<RowImportModel>
        {
            public int Uid;
            public string User;
            public string /*DateTime*/ Timestamp;
            public long Changeset;

            // Default comparer for type.
            public int CompareTo(RowImportModel compareRow)
            {
                // A null value means that this object is greater.
                if (compareRow == null)
                    return 1;

                else
                {
                    var uc = this.Uid.CompareTo(compareRow.Uid);
                    if (uc == 0) return this.Timestamp.CompareTo(compareRow.Timestamp);
                    else return uc;
                }
            }
        }

        public class RowExportModel : IComparable<RowExportModel>
        {
            public int Uid;
            public string User;
            public string /*DateTime*/ Timestamp;
            public int ChangesetsCount;

            // Default comparer for type.
            public int CompareTo(RowExportModel compareRow)
            {
                // A null value means that this object is greater.
                if (compareRow == null)
                    return 1;
                else return this.User.CompareTo(compareRow.User);
            }

            public static string ToCsvHeader()
            {
                return "uid;user;timestamp;changesets_count";
            }

            public void WriteCsvRow(StreamWriter writer)
            {
                writer.Write(Uid);
                writer.Write(';');
                if (User.Contains(';'))
                {
                    writer.Write('"');
                    writer.Write(User);
                    writer.Write('"');
                }
                else writer.Write(User);
                writer.Write(';');
                writer.Write(Timestamp);
                writer.Write(';');
                writer.Write(ChangesetsCount);
                writer.WriteLine();
            }
        }

        public class RowAnalizeModel : IEquatable<RowAnalizeModel>
        {
            public int Uid;
            public string User;
            public int ChangesetsCount;
            public int ChainDays;

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                RowAnalizeModel objAsRow = obj as RowAnalizeModel;
                if (objAsRow == null) return false;
                else return Equals(objAsRow);
            }

            public bool Equals(RowAnalizeModel other)
            {
                if (other == null) return false;
                return (this.Uid.Equals(other.Uid));
            }

            public override int GetHashCode()
            {
                return Uid;
            }

            public static string ToCsvHeader()
            {
                return "uid;user;changesets_count;chain_days";
            }

            public void WriteCsvRow(StreamWriter writer)
            {
                writer.Write(Uid);
                writer.Write(';');
                if (User.Contains(';'))
                {
                    writer.Write('"');
                    writer.Write(User);
                    writer.Write('"');
                }
                else writer.Write(User);
                writer.Write(';');
                writer.Write(ChangesetsCount);
                writer.Write(';');
                writer.Write(ChainDays);
                writer.WriteLine();
            }
        }

        static void Main(string[] args)
        {
            if (args.Length < 1 || args.Length > 3)
            {
                Console.WriteLine("USAGE: osmeveryday.exe change_day.osc\n\n");
                Console.WriteLine("       osmeveryday.exe /analize path-to-dir-on-csv\n\n");
                return;
            }
            if (args[0] == "/analize")
            {
                var changeDaysDir = args[1];
                Analize(changeDaysDir);
                return;
            }

            var changeDay = args[0];
            if (!File.Exists(changeDay))
            {
                Console.WriteLine("File {0} not found.", Path.GetFullPath(changeDay));
                return;
            }

            var rowsImport = default(List<RowImportModel>);
            switch(Path.GetExtension(changeDay))
            {
                case ".osc":
                    rowsImport = ReadChangeDayOsc(changeDay);
                    break;
                case ".gz":
                    rowsImport = ReadChangeDayOscGz(changeDay);
                    break;
                default:
                    Console.WriteLine("Not supported format detected by extention of file");
                    return;
            }
            
            var rowsExport = PrepareToExport(rowsImport);
            var csv = Path.ChangeExtension(Path.GetFullPath(changeDay), "csv");
            ExportToCsv(csv, rowsExport);

            Console.WriteLine("All changesets: {0}, Export: {1}", rowsImport.Count, rowsExport.Count);
        }

        static List<RowImportModel> ReadChangeDayOscGz(string changeDayGz)
        {
            var rowsImport = new List<RowImportModel>(DEFAULT_ROWS);
            try
            {
                var readerOscGzRaw = new FileStream(changeDayGz, FileMode.Open);
                int dwSize = sizeof(int);
                readerOscGzRaw.Position = readerOscGzRaw.Length - dwSize;    // last DWORD is size len
                var rawSize = new byte[dwSize];
                readerOscGzRaw.Read(rawSize, 0, dwSize);
                long length = BitConverter.ToUInt32(rawSize, 0);
                readerOscGzRaw.Position = 0;

                var readerOscGz = new GZipStream(readerOscGzRaw, CompressionMode.Decompress);
                var readerOsc = new StreamReader(readerOscGz, Encoding.UTF8);

                rowsImport = ParseOsc(readerOsc, length);
                readerOsc.Close();

            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO READ.GZ ERROR: {0}", ioex.Message);
            }

            return rowsImport;
        }

        static List<RowImportModel> ReadChangeDayOsc(string changeDay)
        {
            var rowsImport = default(List<RowImportModel>);
            try
            {
                var readerOsc = new StreamReader(changeDay, Encoding.UTF8);
                rowsImport = ParseOsc(readerOsc, readerOsc.BaseStream.Length);
                readerOsc.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO READ ERROR: {0}", ioex.Message);
            }
            return rowsImport;
        }

        static List<RowImportModel> ParseOsc(StreamReader readerOsc, long fileSize)
        {
            var rowsImport = new List<RowImportModel>(DEFAULT_ROWS);
            var existsChangeset = new HashSet<long>();

            int count = 0;
            //long fileSize = readerOsc.BaseStream.Length;
            long readSize = 0;
            const int progressSize = 28;
            long blockSize = fileSize / progressSize;
            int newLineSize = Environment.NewLine.Length;

            long nextBlock = blockSize;

            Console.WriteLine("Read progress:");
            for (int i = 0; i < progressSize; i++)
            {
                Console.Write('░');
            }
            Console.Write('\r');

            var splitPattern = '"';

            while (!readerOsc.EndOfStream)
            {
                var line = readerOsc.ReadLine();
                readSize += line.Length + newLineSize;
                if (readSize >= nextBlock)
                {
                    nextBlock += blockSize;
                    Console.Write('▓');
                }

                if (line.Contains("<node id=") || line.Contains("<way id=") || line.Contains("<relation id="))
                {
                    var splits = line.Split(splitPattern);
                    if (splits[4] == " timestamp=" && splits[6] == " uid=" && splits[8] == " user=" && splits[10] == " changeset=")
                    {
                        var changeset = long.Parse(splits[11]);
                        if (!existsChangeset.Contains(changeset))
                        {
                            existsChangeset.Add(changeset);
                            rowsImport.Add(new RowImportModel
                            {
                                Timestamp = splits[5],
                                Uid = int.Parse(splits[7]),
                                User = splits[9],
                                Changeset = changeset
                            });
                        }
                    }
                    else
                    {
                        Console.WriteLine();
                        Console.WriteLine("WARNING: bad line - {0}", line);
                    }
                }

                count++;
                //if (count > 100) break;
            }
            Console.WriteLine();

            existsChangeset.Clear();
            return rowsImport;
        }

        static List<RowExportModel> PrepareToExport(List<RowImportModel> rowImport)
        {
            // sort by uid
            rowImport.Sort();

            var rowExport = new List<RowExportModel>((int)(DEFAULT_ROWS * 0.6f));

            RowExportModel rowE = new RowExportModel { Uid = -1 };
            foreach (var rowI in rowImport)
            {
                if (rowI.Uid == rowE.Uid)
                {
                    rowE.ChangesetsCount++;
                    rowE.Timestamp = rowI.Timestamp;
                }
                else
                {
                    if (rowE.Uid != -1) rowExport.Add(rowE);
                    rowE = new RowExportModel
                    {
                        Uid = rowI.Uid,
                        User = rowI.User,
                        Timestamp = rowI.Timestamp,
                        ChangesetsCount = 1
                    };
                }
            }

            rowExport.Sort();

            return rowExport;
        }

        static void ExportToCsv(string csv, List<RowExportModel> rowExport)
        {
            try
            {
                var writeCsv = new StreamWriter(csv, false, Encoding.UTF8);                
                writeCsv.WriteLine(RowExportModel.ToCsvHeader());
                foreach (var rowE in rowExport)
                {
                    rowE.WriteCsvRow(writeCsv);
                }
                writeCsv.Flush();
                writeCsv.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO WRITE EXPORT ERROR: {0}", ioex.Message);
            }
        }

        static void Analize(string pathToDir)
        {
            var filesCsv = Directory.GetFiles(pathToDir, "*.csv");
            filesCsv = filesCsv.Where(f => !f.Contains("-")).ToArray(); // исключить файлы анализа
            Array.Sort(filesCsv);

            var setPrev = new HashSet<RowAnalizeModel>();
            var setCur = default(HashSet<RowAnalizeModel>);

            int chainDays = 1;
            foreach (var fileCsv in filesCsv)
            {
                setPrev = setCur;
                setCur = new HashSet<RowAnalizeModel>();

                var readerCsv = new StreamReader(Path.Combine(pathToDir, fileCsv), Encoding.UTF8);
                var header = readerCsv.ReadLine();
                var columnsHeader = header.Split(';');
                int cUid = Array.FindIndex(columnsHeader, str => str == "uid");
                int cUser = Array.FindIndex(columnsHeader, str => str == "user");
                int cChangesetCount = Array.FindIndex(columnsHeader, str => str == "changesets_count");

                var helperQuoteColumn = new StringBuilder(128);

                while (!readerCsv.EndOfStream)
                {
                    var line = readerCsv.ReadLine();
                    var columns = line.Split(';');
                    var row = new RowAnalizeModel();
                    if (columns.Length == columnsHeader.Length)
                    {
                        row.Uid = int.Parse(columns[cUid]);
                        row.User = columns[cUser];
                        row.ChangesetsCount = int.Parse(columns[cChangesetCount]);
                    }
                    else
                    {
                        // Quote
                        var columnQ = line.Split('"');
                        var curColumn = 0;
                        helperQuoteColumn.Length = 0;
                        bool openQoute = false;
                        for (int i = 0; i < line.Length; i++)
                        {
                            char c = line[i];
                            if (c == ';' && !openQoute || i == line.Length - 1)
                            {
                                if (i == line.Length - 1) helperQuoteColumn.Append(c);

                                if (curColumn == cUid) row.Uid = int.Parse(helperQuoteColumn.ToString());
                                else if (curColumn == cUser) row.User = helperQuoteColumn.ToString();
                                else if (curColumn == cChangesetCount) row.ChangesetsCount = int.Parse(helperQuoteColumn.ToString());

                                helperQuoteColumn.Length = 0;
                                curColumn++;
                            }
                            else if (c == '"')
                            {
                                openQoute = !openQoute;
                            }
                            else helperQuoteColumn.Append(c);
                        }

                    }
                    setCur.Add(row);
                }
                readerCsv.Close();

                if (setPrev != null)
                {
                    setCur.IntersectWith(setPrev);
                    var tmpDict = setPrev.ToDictionary(el => el.Uid);

                    foreach (var row in setCur)
                    {
                        row.ChainDays = chainDays;
                        row.ChangesetsCount += tmpDict[row.Uid].ChangesetsCount;
                    }
                    tmpDict.Clear();

                    var prev = Path.GetFileNameWithoutExtension(filesCsv[0]);
                    var cur = Path.GetFileNameWithoutExtension(fileCsv);
                    var fileResult = string.Format("{0}-{1}.csv", prev, cur);
                    ExportAnalizeToCsv(Path.Combine(pathToDir, fileResult), setCur);
                    Console.WriteLine("Exporting... -> {0} Records: {1}", fileResult, setCur.Count);

                    chainDays++;
                }
            }
        }

        static void ExportAnalizeToCsv(string csv, HashSet<RowAnalizeModel> rowExport)
        {
            try
            {
                var writeCsv = new StreamWriter(csv, false, Encoding.UTF8);
                writeCsv.WriteLine(RowAnalizeModel.ToCsvHeader());
                foreach (var rowE in rowExport)
                {
                    rowE.WriteCsvRow(writeCsv);
                }
                writeCsv.Flush();
                writeCsv.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO WRITE ANALIZE ERROR: {0}", ioex.Message);
            }
        }
    }
}
