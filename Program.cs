using System;
using System.Collections.Generic;
using System.IO;
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
        }


        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("USAGE: osmeveryday.exe change_day.osc\n\n");
                return;
            }
            var change_day = args[0];
            if (!File.Exists(change_day))
            {
                Console.WriteLine("File {0} not found.", Path.GetFullPath(change_day));
                return;
            }

            var rowsImport = ReadChangeDayOsc(change_day);
            var rowsExport = PrepareToExport(rowsImport);
            var csv = Path.ChangeExtension(Path.GetFullPath(change_day), "csv");
            ExportToCsv(csv, rowsExport);

            Console.WriteLine("All changesets: {0}, Export: {1}", rowsImport.Count, rowsExport.Count);
        }

        static List<RowImportModel> ReadChangeDayOsc(string change_day)
        {
            var rowsImport = new List<RowImportModel>(DEFAULT_ROWS);
            var existsChangeset = new HashSet<long>();

            try
            {
                var readerOsc = new StreamReader(change_day);
                int count = 0;
                long fileSize = readerOsc.BaseStream.Length;
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
                readerOsc.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO READ ERROR: {0}", ioex.Message);
            }

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
                var writeCsv = new StreamWriter(csv);
                writeCsv.WriteLine("uid;user;timestamp;changesets_count");
                foreach (var rowE in rowExport)
                {
                    writeCsv.Write(rowE.Uid);
                    writeCsv.Write(';');
                    if (rowE.User.Contains(';'))
                    {
                        writeCsv.Write('"');
                        writeCsv.Write(rowE.User);
                        writeCsv.Write('"');
                    }
                    else writeCsv.Write(rowE.User);
                    writeCsv.Write(';');
                    writeCsv.Write(rowE.Timestamp);
                    writeCsv.Write(';');
                    writeCsv.Write(rowE.ChangesetsCount);
                    writeCsv.WriteLine();
                }
                writeCsv.Flush();
                writeCsv.Close();
            }
            catch (IOException ioex)
            {
                Console.WriteLine("IO WRITE ERROR: {0}", ioex.Message);
            }
        }
    }
}
