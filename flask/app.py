from flask import Flask, jsonify, render_template, request, flash, redirect, session, url_for;
from flask_cors import CORS;
from flask_sqlalchemy import SQLAlchemy;
import requests;
from markupsafe import escape;
from flask_user import login_required, UserManager, UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
CORS(app)
ENV = 'prod'

# Setting database configs
if ENV == 'dev':
    app.debug = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:Biology512@localhost/book_recs'
else:
    app.debug = False
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://grrxtpxtklabjt:19192f867330d309d07c38acd0d7f79dc1fef4ccafc757e9e25245d03f54ba20@ec2-52-204-232-46.compute-1.amazonaws.com:5432/d7fl9nj50gmm5f'

app.config['SQL_ALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['SECRET_KEY'] = "OCML3CRawVEueaxcuKHOph"

db = SQLAlchemy(app)

# Building user model
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(200))
    password = db.Column(db.String(200))
    password_hash = db.Column(db.String(200))

    def __init__(self, username, password, password_hash):
        self.username = username
        self.password = password
        self.password_hash = password_hash


# Building ratings model
class Ratings(db.Model):
    __tablename__ = 'ratings'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(200))
    rating = db.Column(db.Integer)
    isbn10 = db.Column(db.String(200))

    def __init__(self, username, rating, isbn10):
        self.username = username
        self.rating = rating
        self.isbn10 = isbn10


# Existing ratings table


# Building routes for the site


# Home page
@app.route('/')
def index():
   return render_template("base.html")

# Registration page
@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        password_hash = generate_password_hash(password)

        if username == '' or password == '':
            return render_template('register.html', message='Please include a name and/or password.')

        if db.session.query(User).filter(User.username == username).count() == 0:
            user = User(username, password, password_hash)
            db.session.add(user)
            db.session.commit()
            session['username'] = user.username
            return redirect(url_for('search'))
        else:
            return render_template('register.html', message='Sorry, this username is already taken.')

        return render_template('register.html')
    
    else:
        return render_template('register.html')


@app.route('/sign-in', methods=["GET", "POST"])
def sign_in():
    if request.method == 'POST':
        username_entered = request.form['username']
        password_entered = request.form['password']
        user = db.session.query(User).filter(User.username == username_entered).first()
        if user is not None and check_password_hash(user.password_hash, password_entered):
            session['username'] = user.username
            return redirect(url_for('search'))
        return render_template('signin.html', message="Sorry, either your username does not exist or your password does not match.")
    else:
        return render_template('signin.html')

@app.route('/sign-out', methods=["GET", "POST"])
def sign_out():
    if request.method == 'POST':
        session.pop('username', None)
        return redirect(url_for('sign_in'))
    else:
        return render_template('signout.html')


@app.route("/search", methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
    # ensure title of book was submitted
        if not request.form.get("title"):
            return("Please enter a book title below.")

        # if user provided author of the book
        if request.form.get("author"):
            """
            return rendered result.html page with books from Google Books API written by the provided author that match
            search results
            """
            return render_template("searchResults.html", books = requests.get("https://www.googleapis.com/books/v1/volumes?q=" +
                               request.form.get("title") + "+inauthor:" + request.form.get("author") +
                               "&key=AIzaSyDIVPxEVrrJxhILbUWL2Nnbcp-bBqBU4vg").json())

        # return rendered result.html page with books from Google Books API that match search results
        return render_template("searchResults.html", books = requests.get("https://www.googleapis.com/books/v1/volumes?q=" +
                               request.form.get("title") + "&key=AIzaSyDIVPxEVrrJxhILbUWL2Nnbcp-bBqBU4vg").json())

    # else if user reached route via GET (as by clicking a link or via redirect)
    else:
        username = session.get('username')
        # return rendered search.html page
        return render_template("search.html", username = username)


# book details route
@app.route("/bookDetails/<book_id>")
def bookDetails(book_id):

    # get book from Google Books API that user choose from home page
    book = requests.get("https://www.googleapis.com/books/v1/volumes?q=" + book_id + "&key=AIzaSyDIVPxEVrrJxhILbUWL2Nnbcp-bBqBU4vg").json()

    """
    return rendered bookDetails.html page with book from Google Books API that user choose from home page and with number of
    users that read that book, average grade of that book and all comments and grades for that book
    """
    return render_template("bookDetails.html", book = book["items"][0])



@app.route("/new-rating", methods=['POST'])
def postnew():
    if request.method == 'POST':
        username = session.get('username')
        rating = request.form['rating']
        isbn13 = request.form.get('isbn13')
        isbn10 = request.form.get('isbn10')

        if db.session.query(Ratings).filter(Ratings.isbn13 == isbn13).count() == 0:
            
            if len(isbn13) == 13:
                pass
            elif len(isbn13) == 10 or len(isbn10) == 13:
                isbn13_old = isbn13
                isbn10_old = isbn10
                isbn10 = isbn13_old
                isbn13 = isbn10_old
            elif len(isbn13) == 0:
                isbn13 = 'no id'
            elif len(isbn10) == 0:
                isbn10 = 'no id'
            else:
                pass
          
            data = Ratings(username, rating, isbn10)
            db.session.add(data)
            db.session.commit()
            return render_template('success.html')
        return render_template('search.html', message='You have already submitted ratings for this book!')



if __name__ == '__main__':
    app.run()