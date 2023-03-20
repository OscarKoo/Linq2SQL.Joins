using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Dao.Linq2SQL.Joins
{
    public static class Linq2SQLJoins
    {
        public static IQueryable<TResult> LeftJoin<TLeft, TRight, TKey, TJoin, TResult>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector,
            Expression<Func<TLeft, IEnumerable<TRight>, TJoin>> joinSelector,
            Expression<Func<TJoin, IEnumerable<TRight>>> requireDefaultIfEmpty,
            Expression<Func<TJoin, TRight, TResult>> resultSelector) =>
            left.GroupJoin(right, leftKeySelector, rightKeySelector, joinSelector).SelectMany(requireDefaultIfEmpty, resultSelector);

        public static IQueryable<Join<TLeft, TRight>> LeftJoin<TLeft, TRight, TKey>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector) =>
            left.LeftJoin(right, leftKeySelector, rightKeySelector,
                (l, r) => new Join<TLeft, IEnumerable<TRight>> { Left = l, Right = r }, lr => lr.Right.DefaultIfEmpty(),
                (lr, r) => new Join<TLeft, TRight> { Left = lr.Left, Right = r });

        public static IQueryable<TResult> RightJoin<TLeft, TRight, TKey, TJoin, TResult>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector,
            Expression<Func<TRight, IEnumerable<TLeft>, TJoin>> joinSelector,
            Expression<Func<TJoin, IEnumerable<TLeft>>> requireDefaultIfEmpty,
            Expression<Func<TJoin, TLeft, TResult>> resultSelector) =>
            right.GroupJoin(left, rightKeySelector, leftKeySelector, joinSelector).SelectMany(requireDefaultIfEmpty, resultSelector);

        public static IQueryable<Join<TLeft, TRight>> RightJoin<TLeft, TRight, TKey>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector) =>
            left.RightJoin(right, leftKeySelector, rightKeySelector,
                (r, l) => new Join<IEnumerable<TLeft>, TRight> { Left = l, Right = r }, lr => lr.Left.DefaultIfEmpty(),
                (lr, l) => new Join<TLeft, TRight> { Left = l, Right = lr.Right });

        public static IQueryable<TResult> FullJoin<TLeft, TRight, TKey, TLeftJoin, TRightJoin, TResult>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector,
            Expression<Func<TLeft, IEnumerable<TRight>, TLeftJoin>> leftJoinSelector,
            Expression<Func<TLeftJoin, IEnumerable<TRight>>> rightRequireDefaultIfEmpty,
            Expression<Func<TLeftJoin, TRight, TResult>> leftResultSelector,
            Expression<Func<TRight, IEnumerable<TLeft>, TRightJoin>> rightJoinSelector,
            Expression<Func<TRightJoin, IEnumerable<TLeft>>> leftRequireDefaultIfEmpty,
            Expression<Func<TRightJoin, TLeft, TResult>> rightResultSelector) =>
            left.LeftJoin(right, leftKeySelector, rightKeySelector, leftJoinSelector, rightRequireDefaultIfEmpty, leftResultSelector)
                .Union(left.RightJoin(right, leftKeySelector, rightKeySelector, rightJoinSelector, leftRequireDefaultIfEmpty, rightResultSelector));

        public static IQueryable<Join<TLeft, TRight>> FullJoin<TLeft, TRight, TKey>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TKey>> leftKeySelector,
            Expression<Func<TRight, TKey>> rightKeySelector) =>
            left.FullJoin(right, leftKeySelector, rightKeySelector,
                (l, r) => new Join<TLeft, IEnumerable<TRight>> { Left = l, Right = r }, lr => lr.Right.DefaultIfEmpty(),
                (lr, r) => new Join<TLeft, TRight> { Left = lr.Left, Right = r },
                (r, l) => new Join<IEnumerable<TLeft>, TRight> { Left = l, Right = r }, lr => lr.Left.DefaultIfEmpty(),
                (lr, r) => new Join<TLeft, TRight> { Left = r, Right = lr.Right });

        public static IQueryable<TResult> CrossJoin<TLeft, TRight, TResult>(this IQueryable<TLeft> left,
            IQueryable<TRight> right,
            Expression<Func<TLeft, TRight, TResult>> resultSelector) =>
            left.SelectMany(e => right, resultSelector);

        public static IQueryable<Join<TLeft, TRight>> CrossJoin<TLeft, TRight>(this IQueryable<TLeft> left,
            IQueryable<TRight> right) =>
            left.CrossJoin(right, (l, r) => new Join<TLeft, TRight> { Left = l, Right = r });

        public static IQueryable<TResult> CrossApply<TLeft, TRight, TResult>(this IQueryable<TLeft> left,
            Expression<Func<TLeft, IEnumerable<TRight>>> rightRequireTake,
            Expression<Func<TLeft, TRight, TResult>> resultSelector) =>
            left.SelectMany(rightRequireTake, resultSelector);

        public static IQueryable<TResult> OuterApply<TLeft, TRight, TResult>(this IQueryable<TLeft> left,
            Expression<Func<TLeft, IEnumerable<TRight>>> rightRequireTakeAndDefaultIfEmpty,
            Expression<Func<TLeft, TRight, TResult>> resultSelector) =>
            left.CrossApply(rightRequireTakeAndDefaultIfEmpty, resultSelector);
    }

    public class Join<TLeft, TRight>
    {
        public TLeft Left { get; set; }
        public TRight Right { get; set; }
    }
}