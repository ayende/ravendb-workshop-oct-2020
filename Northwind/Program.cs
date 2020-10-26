using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Linq;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;

namespace Northwind
{
    class Program
    {
        static void Main(string[] args)
        {
            var certCloud = new X509Certificate2(
                @"C:\Users\ayende\Downloads\ndc.roll.client.certificate\ndc.roll.client.certificate.pfx");
            var certLocal = new X509Certificate2(
                @"C:\Users\ayende\Downloads\ndc-worksop.Cluster.Settings\admin.client.certificate.ndc-worksop.pfx");
            var cloud = new[]
            {
                "https://a.ndc.roll.ravendb.cloud",
                "https://b.ndc.roll.ravendb.cloud",
                "https://c.ndc.roll.ravendb.cloud"
            };
            var local = new[]
            {
                "https://a.ndc-worksop.ravendb.community/",
                "https://b.ndc-worksop.ravendb.community/",
                "https://c.ndc-worksop.ravendb.community/",
            };
            using var store = new DocumentStore
            {
                Database = "Demo",
                Certificate = certCloud,
                Urls = cloud
            };
            store.Initialize();

            new Employees_SearchNotes().Execute(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<Employees_SearchNotes.Result, Employees_SearchNotes>()
                    .Search(x => x.Name, "Anne")
                    .OfType<Employee>()
                    .ToList();
                foreach (var result in results)
                {
                    Console.WriteLine(result.FirstName);
                }

            }

            //var sp = Stopwatch.StartNew();
            //using (var session = store.OpenSession())
            //{
            //    var results = session.Query<Company>()
            //        .Search(x => x.Contact.Name, "Karl")
            //        .ToList();
            //    foreach (var result in results)
            //    {
            //        Console.WriteLine(result.Name);
            //    }

            //}

            //Console.WriteLine(sp.Elapsed);
        }
    }

    public class Employees_SearchNotes : AbstractIndexCreationTask<Employee, Employees_SearchNotes.Result>
    {
        public class Result
        {
            public string Notes;
            public string Name;
        }
        public Employees_SearchNotes()
        {
            Map = employees => from emp in employees
                select new
                {
                    emp.Notes,
                    Name = new[]
                    {
                        emp.FirstName,
                        emp.LastName
                    }
                };
            Index(x => x.Notes, FieldIndexing.Search);
            Index(x=>x.Name, FieldIndexing.Search);
        }
    }

    public class Contract
    {
        public string Id { get; set; }
        public DateTime DateCreated { get; set; }
        public DateTime ValidFrom { get; set; }
        public DateTime ValidTo { get; set; }
        public decimal HourRate { get; set; }
    }

    public class Paystub
    {
        public string ContractId;
        public string TaxRateId;

        public string EmployeeId;
        public string EmployeeName;

        public string BankDetails;

        public List<DateRange> Hours = new List<DateRange>();
        public List<(string Desc, decimal Amount)> Lines = new List<(string Desc, decimal Amount)>();
    }

    public class DateRange
    {
        public DateTime Start, End;

        public DateRange()
        {

        }

        public DateRange(DateTime start, DateTime end)
        {
            Start = start;
            End = end;
        }
    }

}
