using System;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;

namespace Northwind
{
    class Program
    {
        static void Main(string[] args)
        {
            var cert = new X509Certificate2(
                @"C:\Users\ayende\Downloads\ndc.roll.client.certificate\ndc.roll.client.certificate.pfx");
            using var store = new DocumentStore
            {
                Database = "Demo",
                Certificate = cert,
                Urls = new[]
                {
                    "https://a.ndc.roll.ravendb.cloud",
                    "https://b.ndc.roll.ravendb.cloud",
                    "https://c.ndc.roll.ravendb.cloud"
                }
            };
            store.Initialize();

            using (var session = store.OpenSession())
            {
                var employee = session.Load<Employee>("employees/1-A");
                Console.WriteLine(employee.FirstName);
            }

        }
    }
}
