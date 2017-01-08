using ICSharpCode.SharpZipLib.BZip2;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;

namespace OsmEveryDay
{
    class Program
    {
        const int DEFAULT_ROWS = 1000;

        public interface IExportToCsv
        {
            string ToCsvHeader();
            void WriteCsvRow(StreamWriter writer);
        }

        public class RowImportStore : IComparable<RowImportStore>
        {
            public int Uid;
            public string User;
            public string /*DateTime*/ Timestamp;
            public long Changeset;

            // Default comparer for type.
            public int CompareTo(RowImportStore compareRow)
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

        public class RowExportStore : IExportToCsv, IComparable<RowExportStore>
        {
            public int Uid;
            public string User;
            public string /*DateTime*/ Timestamp;
            public int ChangesetsCount;

            // Default comparer for type.
            public int CompareTo(RowExportStore compareRow)
            {
                // A null value means that this object is greater.
                if (compareRow == null)
                    return 1;
                else return this.User.CompareTo(compareRow.User);
            }

            public string ToCsvHeader()
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

        public class RowAnalizeStore : IExportToCsv, IEquatable<RowAnalizeStore>
        {
            public int Uid;
            public string User;
            public int ChangesetsCount;
            public int ChainDays;

            public override bool Equals(object obj)
            {
                if (obj == null) return false;
                RowAnalizeStore objAsRow = obj as RowAnalizeStore;
                if (objAsRow == null) return false;
                else return Equals(objAsRow);
            }

            public bool Equals(RowAnalizeStore other)
            {
                if (other == null) return false;
                return (this.Uid.Equals(other.Uid));
            }

            public override int GetHashCode()
            {
                return Uid;
            }

            public string ToCsvHeader()
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

            public override string ToString()
            {
                return string.Format("{0}[{1}]:{2}/{3}", User, Uid, ChangesetsCount, ChainDays);
            }
        }

        static void ShowHelp()
        {
            Console.WriteLine("USAGE: osmeveryday.exe change_day.osc\n\n");
            Console.WriteLine("       osmeveryday.exe /analize path-to-dir-on-csv\n\n");
            Console.WriteLine("       osmeveryday.exe /analize changesets-latest.osm.bz2 <year>\n");
            Console.WriteLine("       osmeveryday.exe /analize changesets-latest.osm.bz2 <year>-<month>\n");
            Console.WriteLine("       osmeveryday.exe /analize changesets-latest.osm.bz2 <year>-<month>-<day>\n\n");
        }

        static void Main(string[] args)
        {
            if (args.Length < 1 || args.Length > 3)
            {
                ShowHelp();
                return;
            }

            var gbDateInfo = new DateTimeFormatInfo();
            gbDateInfo.ShortDatePattern = "yyyy-MM-dd";
            var culture = (CultureInfo)CultureInfo.CurrentCulture.Clone();
            culture.DateTimeFormat = gbDateInfo;
            Thread.CurrentThread.CurrentCulture = culture;

            if (args[0] == "/analize")
            {
                var changeDaysDir = args[1];
                var changesetsFile = Path.GetFileName(changeDaysDir);
                if (changesetsFile.StartsWith("changesets") && changesetsFile.EndsWith("osm.bz2"))
                {
                    if (args.Length != 3)
                    {
                        ShowHelp();
                        return;
                    }
                    AnalizeYearByChangesetAll(changeDaysDir, args[2]);
                }
                else
                {
                    Analize(changeDaysDir);
                }
                return;
            }

            var changeDay = args[0];
            if (!File.Exists(changeDay))
            {
                Console.WriteLine("File {0} not found.", Path.GetFullPath(changeDay));
                return;
            }

            var rowsImport = default(List<RowImportStore>);
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

        static List<RowImportStore> ReadChangeDayOscGz(string changeDayGz)
        {
            var rowsImport = new List<RowImportStore>(DEFAULT_ROWS);
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

        static List<RowImportStore> ReadChangeDayOsc(string changeDay)
        {
            var rowsImport = default(List<RowImportStore>);
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

        static List<RowImportStore> ParseOsc(StreamReader readerOsc, long fileSize)
        {
            var rowsImport = new List<RowImportStore>(DEFAULT_ROWS);
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
                            rowsImport.Add(new RowImportStore
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

        //static List<RowExportStore> PrepareToExport(List<RowImportStore> rowImport)
        static List<RowExportStore> PrepareToExport(List<RowImportStore> rowImport)
        {
            // sort by uid
            rowImport.Sort();

            var rowExport = new List<RowExportStore>((int)(DEFAULT_ROWS * 0.6f));

            RowExportStore rowE = new RowExportStore { Uid = -1 };
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
                    rowE = new RowExportStore
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

        static void ExportToCsv<T>(string csv, ICollection<T> rowExport) where T : IExportToCsv
        {
            if (rowExport.Count == 0) return;
            try
            {
                var writeCsv = new StreamWriter(csv, false, Encoding.UTF8);                
                writeCsv.WriteLine(rowExport.First().ToCsvHeader());
                foreach (var rowE in rowExport)
                {
                    rowE.WriteCsvRow(writeCsv);
                }
                writeCsv.Flush();
                writeCsv.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO WRITE EXPORT ({0}) ERROR: {1}", rowExport.First().GetType(), ioex.Message);
            }
        }

        static void Analize(string pathToDir)
        {
            var filesCsv = default(string[]);
            try
            {
                filesCsv = Directory.GetFiles(pathToDir, "*.csv");
            } catch (IOException ioex)
            {
                Console.Error.WriteLine("Dir not found: {0}", pathToDir);
                return;
            }
            filesCsv = filesCsv.Where(f => !f.Contains("-")).ToArray(); // исключить файлы анализа
            Array.Sort(filesCsv);

            var setPrev = new HashSet<RowAnalizeStore>();
            var setCur = default(HashSet<RowAnalizeStore>);

            int chainDays = 1;
            foreach (var fileCsv in filesCsv)
            {
                setPrev = setCur;
                setCur = new HashSet<RowAnalizeStore>();

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
                    var row = new RowAnalizeStore();
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
                    if (!File.Exists(fileResult))
                    {
                        //ExportAnalizeToCsv(Path.Combine(pathToDir, fileResult), setCur);
                        ExportToCsv(Path.Combine(pathToDir, fileResult), setCur);
                        Console.WriteLine("Exporting... -> {0} Records: {1}", fileResult, setCur.Count);
                    }
                    chainDays++;
                }
            }
        }

        /*static void ExportAnalizeToCsv(string csv, HashSet<RowAnalizeStore> rowExport)
        {
            try
            {
                var writeCsv = new StreamWriter(csv, false, Encoding.UTF8);
                writeCsv.WriteLine(RowAnalizeStore.ToCsvHeader());
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
        }*/

        public enum SearchState
        {
            No,
            Found,
            Complete
        }

        static void AnalizeYearByChangesetAll(string fileChangesetAll, string year)
        {
            var readerOsmBZip2Raw = new FileStream(fileChangesetAll, FileMode.Open);

            string dayStart = string.Empty;
            string dayEnd = string.Empty;
            var dayCurrent = default(DateTime);

            try
            {
                switch (year.Length)
                {
                    case 4: // год
                        var daySY = new DateTime(int.Parse(year.Substring(0, 4)), 1, 1);
                        var dayEY = daySY.AddYears(1);
                        dayStart = daySY.ToShortDateString();
                        dayEnd = dayEY.ToShortDateString();
                        dayCurrent = daySY;
                        break;
                    case 7: // месяц
                        var daySM = new DateTime(int.Parse(year.Substring(0, 4)), int.Parse(year.Substring(5, 2)), 1);
                        var dayEM = daySM.AddMonths(1);
                        dayStart = daySM.ToShortDateString();
                        dayEnd = dayEM.ToShortDateString();
                        dayCurrent = daySM;
                        break;
                    case 10: // день
                        var daySD = new DateTime(int.Parse(year.Substring(0, 4)), int.Parse(year.Substring(5, 2)), int.Parse(year.Substring(8, 2)));
                        dayStart = dayEnd = daySD.ToShortDateString();
                        dayCurrent = daySD;
                        break;
                    default:
                        Console.WriteLine("Wrong YEAR: {0}", year);
                        return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error parse YEAR: {0}\n{1}", year, ex.Message);
                return;
            }

            if (dayCurrent < new DateTime(2009, 4, 21))
            {
                Console.WriteLine("Changesets before 2009-04-21 not supported. Exit.");
                return;
            }
            
            string backupLine = string.Empty;
            string curDay = dayCurrent.ToShortDateString();
            int chainDays = 1;

            var searchState = SearchState.No;

            var indexBZip2 = GetOrCreateIndexBZip2Achives(readerOsmBZip2Raw);

            Console.Write("Progress: {0} - ", curDay);

            var prevDaySet = default(HashSet<RowAnalizeStore>);
            var curDaySet = new HashSet<RowAnalizeStore>();
            var yearSet = new HashSet<RowAnalizeStore>();
            var hashTableByUid = new Hashtable();
            var prevHashTableByUid = new Hashtable();

            //var bIndex = indexBZip2.BinarySearch(new BZip2OsmIndex { StartDateTime = dayCurrent });
            //while (indexBZip2[--bIndex].StartDateTime == dayCurrent)
            //{ }
            //readerOsmBZip2Raw.Position = indexBZip2[bIndex + 1].Position;
            var prevBlockIndex = indexBZip2.FindIndex(el => el.StartDateTime == dayCurrent) - 1;
            readerOsmBZip2Raw.Position = indexBZip2[prevBlockIndex].Position;

            while (readerOsmBZip2Raw.Length > readerOsmBZip2Raw.Position && searchState != SearchState.Complete)
            {
                var readerOsmBZip2 = new BZip2InputStream(readerOsmBZip2Raw);
                readerOsmBZip2.IsStreamOwner = false;
                var readerOsm = new StreamReader(readerOsmBZip2, Encoding.UTF8);

                while (!readerOsm.EndOfStream && searchState != SearchState.Complete)
                {
                    var line = readerOsm.ReadLine();
                    if (readerOsm.EndOfStream)
                    {
                        backupLine = line;
                        continue;
                    }
                    if (backupLine.Length > 0)
                    {
                        line = backupLine + line;
                        backupLine = string.Empty;
                    }

                    if (line.Contains("<changeset id="))
                    {
                        var splits = line.Split('"');
                        if (splits[2] == " created_at=")
                        {
                            var date = splits[3].Substring(0, 10);

                            // раньше времени
                            if (date.CompareTo(dayStart) < 0) continue;
                            if (searchState == SearchState.No) searchState = SearchState.Found;

                            if (date != curDay)
                            {
                                if (prevDaySet != null)
                                {
                                    var forYear = new HashSet<RowAnalizeStore>(curDaySet);
                                    curDaySet.IntersectWith(prevDaySet);

                                    forYear.ExceptWith(curDaySet);
                                    yearSet.UnionWith(forYear);

                                    prevDaySet.ExceptWith(curDaySet);
                                    yearSet.UnionWith(prevDaySet);


                                    foreach (var row in curDaySet)
                                    {
                                        row.ChainDays = chainDays;
                                        row.ChangesetsCount += ((RowAnalizeStore)prevHashTableByUid[row.Uid]).ChangesetsCount;
                                    }

                                    prevDaySet.Clear();
                                    prevHashTableByUid.Clear();

                                    if (curDaySet.Count == 0)
                                    {
                                        searchState = SearchState.Complete;
                                    }

                                    Console.SetCursorPosition(Console.CursorLeft - 10, Console.CursorTop);
                                }

                                chainDays++;

                                Console.Write(curDay);

                                if (searchState == SearchState.Found)
                                {
                                    prevDaySet = curDaySet;
                                    prevHashTableByUid = hashTableByUid;
                                }

                                curDaySet = new HashSet<RowAnalizeStore>();
                                hashTableByUid = new Hashtable();

                                dayCurrent = dayCurrent.AddDays(1);
                                curDay = dayCurrent.ToShortDateString();
                            }

                            if (searchState == SearchState.Found && date == curDay && date.CompareTo(dayEnd) < 0)
                            {
                                // в противном случае ананимные правки
                                if (splits.Length >= 10 && splits[10] == " uid=" && splits[8] == " user=")
                                {
                                    var uid = int.Parse(splits[11]);

                                    if (!hashTableByUid.ContainsKey(uid))
                                    {
                                        var row = new RowAnalizeStore
                                        {
                                            Uid = uid,
                                            User = splits[9],
                                            ChangesetsCount = 1
                                        };
                                        curDaySet.Add(row);
                                        hashTableByUid.Add(uid, row);
                                    }
                                    else
                                    {
                                        var rowO = (RowAnalizeStore)hashTableByUid[uid];
                                        rowO.ChangesetsCount++;
                                    }
                                }
                            }
                            else
                            {
                                // закончен временной промежуток
                                yearSet.UnionWith(prevDaySet);

                                prevDaySet.Clear();
                                prevHashTableByUid.Clear();
                                curDaySet.Clear();

                                var fullP = Path.GetFullPath(fileChangesetAll);
                                var dir = Path.GetDirectoryName(fullP);
                                var fileCsv = string.Format("{0}_{1}.csv", Path.GetFileNameWithoutExtension(fileChangesetAll), year);
                                string fileResult = Path.Combine(dir, fileCsv);
                                if (!File.Exists(fileResult))
                                {
                                    ExportToCsv(fileResult, yearSet);
                                    Console.WriteLine();
                                    Console.WriteLine("Exporting... -> {0} Records: {1}", fileCsv, yearSet.Count);
                                }
                                else 
                                {
                                    Console.WriteLine("Skip: {0}", fileResult);
                                }
                                searchState = SearchState.Complete;
                            }
                        }
                        else
                        {
                            //Console.WriteLine();
                            Console.WriteLine("WARNING: bad line - {0}", line);
                        }
                    }
                }
                readerOsm.Dispose();
                readerOsmBZip2.Dispose();
            }

            readerOsmBZip2Raw.Close();
        }

        public class BZip2OsmIndex: IExportToCsv, IComparable<BZip2OsmIndex>
        {
            public DateTime StartDateTime;
            public long Position;

            // Default comparer for type.
            public int CompareTo(BZip2OsmIndex compareRow)
            {
                // A null value means that this object is greater.
                if (compareRow == null)
                    return 1;

                else
                {
                    return this.StartDateTime.CompareTo(compareRow.StartDateTime);                    
                }
            }

            public string ToCsvHeader()
            {
                return "date_time;position";
            }

            public void WriteCsvRow(StreamWriter writer)
            {
                writer.Write(StartDateTime.ToShortDateString());
                writer.Write(';');
                writer.Write(Position);
                writer.WriteLine();
            }
        }

        static List<BZip2OsmIndex> GetOrCreateIndexBZip2Achives(FileStream readerOsmBZip2Raw)
        {
            var result = new List<BZip2OsmIndex>(20000);

            var fullP = Path.GetFullPath(readerOsmBZip2Raw.Name);
            var dir = Path.GetDirectoryName(fullP);
            var fileCsv = string.Format("{0}_indexBZip2.csv", Path.GetFileNameWithoutExtension(readerOsmBZip2Raw.Name));
            string fileResult = Path.Combine(dir, fileCsv);

            if (File.Exists(fileResult))
            {
                var readerCsv = new StreamReader(fileResult);
                var headerCsv = readerCsv.ReadLine();
                while (!readerCsv.EndOfStream)
                {
                    var lineCsv = readerCsv.ReadLine();
                    var splitsCsv = lineCsv.Split(';');
                    result.Add(new BZip2OsmIndex
                    {
                        StartDateTime = DateTime.Parse(splitsCsv[0]),
                        Position = long.Parse(splitsCsv[1])
                    });
                }
                readerCsv.Close();
                return result;
            }

            const int progressSize = 100;
            long blockSize = readerOsmBZip2Raw.Length / progressSize;

            long nextBlock = blockSize;

            Console.WriteLine("Create index progress:");
            for (int i = 0; i < progressSize; i++)
            {
                Console.Write('░');
            }
            Console.Write('\r');

            byte[] bzip2Signature = { (byte)'B', (byte)'Z', (byte)'h' };

            while (readerOsmBZip2Raw.Position < readerOsmBZip2Raw.Length)
            {
                bool eof = false;
                if (readerOsmBZip2Raw.Position > 0)
                {
                    int signCheck = 0;
                    while (signCheck != 3)
                    {
                        int b = readerOsmBZip2Raw.ReadByte();
                        if (b == -1)
                        {
                            eof = true;
                            break;
                        }
                        if (b == bzip2Signature[signCheck]) signCheck++;
                        else signCheck = 0;
                    }

                    readerOsmBZip2Raw.Seek(-3, SeekOrigin.Current);
                }
                if (eof) break;

                var record  = new BZip2OsmIndex
                {
                    Position = readerOsmBZip2Raw.Position
                };
                var readerOsmBZip2 = new BZip2InputStream(readerOsmBZip2Raw);
                readerOsmBZip2.IsStreamOwner = false;

                var readerOsm = new StreamReader(readerOsmBZip2, Encoding.UTF8);
                while (!readerOsm.EndOfStream)
                {
                    var line = readerOsm.ReadLine();
                    if (line.Contains("<changeset id="))
                    {
                        var splits = line.Split('"');
                        if (splits[4] == " closed_at=")
                        {
                            record.StartDateTime = DateTime.Parse(splits[5]);
                            result.Add(record);
                            break;
                        }
                    }
                }


                if (readerOsmBZip2Raw.Position >= nextBlock)
                {
                    nextBlock += blockSize;
                    Console.Write('▓');
                }

                readerOsm.Dispose();
                readerOsmBZip2.Dispose();
            }
            readerOsmBZip2Raw.Position = 0;

            Console.WriteLine();
            Console.WriteLine("Complete");
            ExportToCsv(fileResult, result);

            return result;
        }
    }
}
