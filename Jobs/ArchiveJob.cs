using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyData.Jobs
{
    public class ArchiveJob : IJob
    {

        Task IJob.Execute(IJobExecutionContext context)
        {
            TickStorage storage = new TickStorage();

            storage.AddToArchive();

            return Task.CompletedTask;
        }
    }
}
