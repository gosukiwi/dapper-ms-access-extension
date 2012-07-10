using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Dapper;
using Model;

namespace Tests
{
    /// <summary>
    /// Test the model's context class
    /// </summary>
    [TestClass]
    public class TestContext
    {
        [TestMethod]
        public void TestConnection()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Assert.AreNotEqual(db, null);
            }
        }

        [TestMethod]
        public void TestBasicCRUD()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Note n = new Note();
                n.Content = "Test content";
                n.Created = DateTime.UtcNow.Date;
                n.Modified = DateTime.UtcNow.Date;

                db.InsertOnSubmit<Note>(n);
                db.SubmitChanges();

                Assert.AreNotEqual(n.Codigo, 0);

                Note result = db.Connection.Query<Note>("SELECT * FROM Notes WHERE Codigo = @Id", new { Id = n.Codigo }).FirstOrDefault();
                Assert.AreNotEqual(result, null);
                Assert.AreEqual(result.Content, n.Content);
                Assert.AreEqual(result.Created, n.Created);
                Assert.AreEqual(result.Modified, n.Modified);

                db.DeleteOnSubmit<Note>(result);
                db.SubmitChanges();

                Assert.IsFalse(db.Connection.Query<Post>("SELECT * FROM Notes WHERE Codigo = @Id", new { Id = n.Codigo }).Any());
            }
        }

        [TestMethod]
        public void TestChainedInsertUsingCollection()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Post p = new Post();
                p.Content = "Test content 'n' escapes!";

                Author a = new Author();
                a.Username = "Mike";
                a.Posts = new List<Post>();
                a.Posts.Add(p);

                int id = db.InsertOnSubmit<Author>(a);

                Assert.AreEqual(a.Id, id);
                Assert.AreEqual(a.Posts.First(), p);

                db.DeleteOnSubmit<Author>(a, Context.CascadeStyle.Collection); // force cascading delete

                db.SubmitChanges();
            }
        }

        [TestMethod]
        public void TestChainedInsertSimple()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Author a = new Author();
                a.Username = "John";

                Post p = new Post();
                p.Content = "Test content";
                p.Author = a;

                int id = db.InsertOnSubmit<Post>(p);

                Assert.AreEqual(id, p.Id);
                Assert.AreEqual(p.Author, a);
                Assert.AreNotEqual(p.Author.Id, 0);

                db.DeleteOnSubmit<Post>(p, Context.CascadeStyle.Single); // force cascading delete

                db.SubmitChanges();
            }
        }

        [TestMethod]
        public void TestUpdateSimple()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Note n = new Note();
                n.Content = "Prueba";
                n.Created = DateTime.UtcNow.Date;
                n.Modified = DateTime.UtcNow.Date;

                db.InsertOnSubmit<Note>(n);
                db.SubmitChanges();

                n.Content = "Test";
                db.UpdateOnSubmit<Note>(n);
                db.SubmitChanges();

                Note newNote = db.Connection.Query<Note>("SELECT * FROM Notes WHERE Codigo = " + n.Codigo).FirstOrDefault();

                Assert.AreEqual(newNote.Content, "Test");

                db.DeleteOnSubmit<Note>(newNote);
                db.SubmitChanges();
            }
        }

        [TestMethod]
        public void TestUpdateCollection()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Author a = new Author();
                a.Username = "Jenny";
                a.Posts = new List<Post>();

                Post p = new Post();
                p.Content = "Test post content";

                Post pp = new Post();
                pp.Content = "Another test content";

                a.Posts.Add(p);
                a.Posts.Add(pp);

                db.InsertOnSubmit<Author>(a); // assume it works, tested on other methods
                db.SubmitChanges();

                a.Posts.First().Content = "Last content";
                db.UpdateOnSubmit<Author>(a, Context.CascadeStyle.All);
                db.SubmitChanges();

                Post compare = db.Connection.Query<Post>("SELECT * FROM Posts WHERE Id = " + a.Posts.First().Id).FirstOrDefault();

                Assert.AreEqual(compare.Content, a.Posts.First().Content);

                db.DeleteOnSubmit<Author>(a, Context.CascadeStyle.All);
                db.SubmitChanges();
            }
        }

        [TestMethod]
        public void TestUpdateSingle()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Author a = new Author();
                a.Username = "John";

                // Now let's use the inserted author
                Post p = new Post();
                p.Content = "Test";
                p.Author = a;

                db.InsertOnSubmit<Post>(p);
                db.SubmitChanges();

                //db.UpdateOnSubmit<Post>(test);
                //db.SubmitChanges();

                //Author compare = db.Connection.Query<Author>("SELECT * FROM Authors WHERE Id = " + test.Author.Id).FirstOrDefault();
                //Assert.AreEqual(compare, test.Author);
            }
        }

        [TestMethod]
        public void TestFind()
        {
            using (Model.Context db = new Model.Context("mdbConnectionString"))
            {
                Post p = new Post();
                p.Content = "Dummy post";

                db.InsertOnSubmit<Post>(p);
                db.SubmitChanges();

                Post found = db.Find<Post>(x => { return x.Content == "Dummy post"; }).FirstOrDefault();

                Assert.AreNotEqual(found, null);
                Assert.AreEqual(found.Content, p.Content);
                Assert.IsTrue(found.Id > 0);
            }
        }
    }

    #region POCOs

    class Note
    {
        [Key]
        public int Codigo { get; set; }
        public string Content { get; set; }
        public DateTime Modified { get; set; }
        public DateTime Created { get; set; }
    }

    class Post
    {
        public int Id { get; set; }
        //[ForeignKey("AuthorId")] // optional foreign key specification, AuthorId is the default value
        public Author Author { get; set; }
        public string Content { get; set; }
    }

    class Author
    {
        public int Id { get; set; }
        public string Username { get; set; }
        public ICollection<Post> Posts { get; set; }
    }

    #endregion
}
