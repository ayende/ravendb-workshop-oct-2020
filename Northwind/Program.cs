using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Raven.Client.Documents.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Subscriptions;

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
                Database = "Sec",
                Certificate = certLocal,
                Urls = local
            };
            store.Initialize();

            var worker = store.Subscriptions.GetSubscriptionWorker<Employee>(new SubscriptionWorkerOptions("LondonEmps")
            {
                Strategy = SubscriptionOpeningStrategy.WaitForFree
            });
            worker.Run(batch =>
            {
                using var session = batch.OpenSession();
                //session.Advanced.UseOptimisticConcurrency = true;
                foreach (var item in batch.Items)
                {
                    Console.WriteLine(item.Id);
                    try
                    {
                        session.Store(new {
                                Employee = item.Id
                            }, "sub/" + item.Id);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        if(e is foo)
                            throw;
                    }
                }
                session.SaveChanges();

            }).Wait();

            //using (var session = store.OpenSession())
            //{
            //    var employee = session.Advanced.Lazily.Load<Employee>("employees/2-A");
            //    var orders = session.Query<Order>()
            //        .Where(x => x.Employee == "employees/2-A")
            //        .OrderByDescending(x => x.OrderedAt)
            //        .Take(5)
            //        .Select(x=>new {
            //            x.Id,
            //            EmpFirstName = RavenQuery.Load<Employee>(x.Employee).FirstName
            //        })
            //        .Lazily();

            //    Console.WriteLine(employee.Value.FirstName);
            //    Console.WriteLine(string.Join(", ", orders.Value.Select(x => x.Id)));
            //    Console.WriteLine(session.Advanced.NumberOfRequests);

            //}



            //IndexCreation.CreateIndexes(Assembly.GetExecutingAssembly(), store);
            //using (var session = store.OpenSession())
            //using(var bulk = store.BulkInsert())
            //{
            //    foreach (var i in session.Query<Employee>().ToList())
            //    {
            //        bulk.Store(i, i.Id);
            //    }
            //    foreach (var i in session.Query<Supplier>().ToList())
            //    {
            //        bulk.Store(i, i.Id);
            //    }
            //    foreach (var i in session.Query<Company>().ToList())
            //    {
            //        bulk.Store(i, i.Id);
            //    }
            //    return;
            //}

            //using (var session = store.OpenSession())
            //{
            //    var ravenQueryable = session.Query<People_Search.Result, People_Search>()
            //        //.Where(x=>x.Name == "anne")
            //        .Search(x => x.Name, "anne")
            //        .Select(x=>new {
            //            Name = x.Contact.Name ?? x.FirstName + " " + x.LastName
            //        });
            //    Console.WriteLine(ravenQueryable);
            //    var results = ravenQueryable
            //        .ToList();

            //    foreach (var result in results)
            //    {
            //        Console.WriteLine(result);
            //        //switch (result)
            //        //{
            //        //    case Employee e:
            //        //        Console.WriteLine(e.FirstName + " " + e.LastName);
            //        //        break;
            //        //    case Supplier s:
            //        //        Console.WriteLine(s.Contact.Name);
            //        //        break;
            //        //    case Company c:
            //        //        Console.WriteLine(c.Contact.Name);
            //        //        break;
            //        //}
            //    }
            //}

            //using (var session = store.OpenSession())
            //{
            //    //session.Include<Order>(x => x.Employee).Load("orders/812-a");

            //    var ravenQueryable = session.Query<Order>()
            //        .Select(o => new
            //        {
            //            Count = o.Lines.Sum(x => x.Quantity),
            //            Total = o.Lines.Sum(x => x.PricePerUnit * x.Quantity)
            //        });
            //    Console.WriteLine(ravenQueryable);
            //    var results = ravenQueryable.ToList();
            //    foreach (var result in results)
            //    {
            //        Console.WriteLine(result);
            //    }

            //}


            //using (var session = store.OpenSession())
            //{
            //    var results = session.Query<Orders_TotalCost.Result, Orders_TotalCost>()
            //        .Where(x=>x.Total > 100)
            //        .OfType<Order>()
            //        .ToList();
            //    foreach (var result in results)
            //    {
            //        Console.WriteLine(result.Id);
            //    }

            //}


            //using (var session = store.OpenSession())
            //{
            //    var results = session.Query<Employees_SearchNotes.Result, Employees_SearchNotes>()
            //        .Search(x => x.Name, "Anne")
            //        .OfType<Employee>()
            //        .ToList();
            //    foreach (var result in results)
            //    {
            //        Console.WriteLine(result.FirstName);
            //    }

            //}

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

    public class Products_ByCountry : AbstractIndexCreationTask<Product, Products_ByCountry.Result>
    {
        public class Result
        {
            public string Country;
            public int Amount;
        }

        public Products_ByCountry()
        {
            Map = products =>
                from product in products
                select new Result
                {
                    Amount = 1,
                    Country = LoadDocument<Supplier>(product.Supplier).Address.Country.ToUpper()
                };
            Reduce = results =>
                from result in results
                group result by result.Country
                into g
                select new Result
                {
                    Country = g.Key,
                    Amount = g.Sum(x => x.Amount)
                };
        }
    }

    public class Products_Recommendations : AbstractIndexCreationTask<Order, Products_Recommendations.Result>
    {
        public class Result
        {
            public string ProductId;
            public int Quantity;

            public List<ProductPurchased> RelatedList;
        }

        public class ProductPurchased
        {
            public string ProductId;
            public int Quantity;
        }

        public Products_Recommendations()
        {
            Map = orders =>
                from order in orders
                from line in order.Lines
                select new Result
                {
                    Quantity = line.Quantity,
                    ProductId = line.Product,
                    RelatedList = order.Lines.Where(x => x.Product != line.Product)
                        .Select(x => new ProductPurchased
                        {
                            ProductId = x.Product,
                            Quantity = x.Quantity
                        }).ToList()
                };

            Reduce = results =>
                from result in results
                group result by result.ProductId
                into g
                select new Result
                {
                    ProductId = g.Key,
                    Quantity = g.Sum(x => x.Quantity),
                    RelatedList = g.SelectMany(x => x.RelatedList)
                        .GroupBy(x => x.ProductId)
                        .Select(g2 => new ProductPurchased
                        {
                            Quantity = g2.Sum(x => x.Quantity),
                            ProductId = g2.Key
                        })
                        .OrderByDescending(x => x.Quantity)
                        .ToList()
                };
        }
    }

    public class Orders_TotalByCompany : AbstractIndexCreationTask<Order, Orders_TotalByCompany.Result>
    {
        public class Result
        {
            public string Company;
            public int NumberOfOrders;
            public int NumberOfProducts;
            public decimal TotalAmountSpent;
        }

        public Orders_TotalByCompany()
        {
            Map = orders => from order in orders
                            select new Result
                            {
                                Company = order.Company,
                                NumberOfOrders = 1,
                                NumberOfProducts = order.Lines.Sum(x => x.Quantity),
                                TotalAmountSpent = order.Lines.Sum(x => x.Quantity * x.PricePerUnit * (1 - x.Discount))
                            };
            Reduce = results => from result in results
                                group result by result.Company
                into g
                                select new Result
                                {
                                    Company = g.Key,
                                    NumberOfProducts = g.Sum(x => x.NumberOfProducts),
                                    NumberOfOrders = g.Sum(x => x.NumberOfOrders),
                                    TotalAmountSpent = g.Sum(x => x.TotalAmountSpent)
                                };
        }
    }

    public class People_Search : AbstractMultiMapIndexCreationTask<People_Search.Result>
    {
        public class Result
        {
            public string Name;

            public Contact Contact;
            public string LastName, FirstName;
        }

        public People_Search()
        {
            AddMap<Employee>(employees =>
                from employee in employees
                select new { Name = new[] { employee.FirstName, employee.LastName } });
            AddMap<Company>(companies => from company in companies
                                         select new { company.Contact.Name });
            AddMap<Supplier>(suppliers => from supplier in suppliers
                                          select new { supplier.Contact.Name });
            Index(x => x.Name, FieldIndexing.Search);
        }
    }

    public class Orders_TotalCost : AbstractIndexCreationTask<Order, Orders_TotalCost.Result>
    {
        public class Result
        {
            public decimal Total;
        }
        public Orders_TotalCost()
        {
            Map = orders => from o in orders
                            select new
                            {
                                Total = o.Lines.Sum(x => x.PricePerUnit * x.Quantity)
                            };
            Store(x => x.Total, FieldStorage.Yes);
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
            Index(x => x.Name, FieldIndexing.Search);
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
